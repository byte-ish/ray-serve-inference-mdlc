# Ray Serve for Inference in the Model Development Life Cycle

A research and reference document on where Ray Serve fits, and does not fit, in an enterprise MDLC, for teams deploying on a cloud platform.

| | |
|---|---|
| **Scope** | Ray Serve as an online inference layer, mapped stage by stage onto the Model Development Life Cycle. Cloud deployment via managed Kubernetes. |
| **Verdict** | Strongest fit for *compositional* inference (multi-model pipelines, RAG, agentic flows) and LLM serving on self-owned accelerators. Over-engineered for a fleet of single-model, tensor-in/tensor-out endpoints. |
| **Confidence** | Moderate. Every claim is documentation-derived or vendor-published. **Nothing here has been executed:** no cluster stood up, no benchmark reproduced. See [Scope and limitations](#9-scope-and-limitations). |
| **Versions** | Ray **2.57.0** (latest stable on PyPI, released 2026-08-11), KubeRay **v1.6.2** |
| **Researched** | 2026-08-12, fact-checked 2026-08-13 |
| **Shelf life** | Short. Several load-bearing capabilities landed in Ray 2.51 to 2.56. Re-verify anything version-gated after roughly two quarters. |

## Key takeaways

1. **Ray Serve owns serving, not the lifecycle.** The Ray docs state plainly that it is not a full ML platform and lacks model lifecycle management and performance visualisation. Registry, lineage, validation evidence, drift monitoring, and CI/CD are all yours to supply. An MDLC that assumes "we have Ray Serve, therefore we have model monitoring" fails its first validation review.
2. **The differentiator is composition, not raw serving throughput.** Arbitrary Python in the request path plus independently-scaled deployments joined by `DeploymentHandle` means a preprocessing/model/postprocessing pipeline is one deployable unit where each node scales on its own bottleneck. If your workload is not compositional, most of the reason to accept Ray's operational cost goes unused.
3. **Upgrade economics are the sharpest adoption risk.** Changing anything in `rayClusterConfig`, including the container image, triggers a blue-green that needs roughly 200% of cluster compute. The efficient alternative, `NewClusterWithIncrementalUpgrade`, is **alpha in KubeRay v1.5.1** behind a feature gate and needs Gateway API plus a controller the docs say is primarily tested with Istio.
4. **Ray 2.56 invalidated older benchmarks.** The throughput path replaced Python-runtime proxy routing with HAProxy, added direct token streaming, and moved Ray out of vLLM's data plane. If your team measured a Ray Serve orchestration tax before 2.56, that measurement is stale.
5. **Two governance edges are specific to Ray Serve.** `user_config` hot-reloads without restarting replicas, so a validated decision threshold can change with no image change and no deploy record. And model multiplexing shares one process across many models, making tenant isolation logical rather than physical.

## Reading guide

| If you want | Read |
|---|---|
| The architecture in ten minutes | [§2 What Ray Serve is, precisely](#2-what-ray-serve-is-precisely) |
| Where it touches your lifecycle and governance | [§3 MDLC mapping](#3-mdlc-mapping) |
| Capacity planning and what drives the bill | [§3.6 Autoscaling and capacity](#36-autoscaling-and-capacity-the-parameters-that-decide-your-bill) |
| A deployment topology to copy | [§4 Cloud reference architecture](#4-cloud-reference-architecture) |
| What will go wrong first | [§5 Failure modes](#5-failure-modes-and-operational-gotchas) |
| What you must build yourself | [§6 What Ray Serve does not give you](#6-what-ray-serve-does-not-give-you) |
| How far to trust this document | [§9 Scope and limitations](#9-scope-and-limitations) |

**Provenance convention.** Claims are either (a) **documented**, traceable to a specific Ray or KubeRay page listed in [§10](#10-sources); (b) **vendor-benchmarked**, from an Anyscale or cloud-provider blog with the benchmark conditions stated so you can judge transfer; or (c) **judgment**, explicitly marked. Anything unmarked is (a). See [§9](#9-scope-and-limitations) for what that does and does not buy you.

---

## 1. Executive summary

Ray Serve is a **Python-native, framework-agnostic online inference layer** built on Ray actors. It is not an ML platform and does not pretend to be one. The Ray docs themselves state it lacks model lifecycle management and model performance visualisation. That boundary is the single most important fact for an MDLC conversation: Ray Serve owns **serving**, and you must supply registry, lineage, validation evidence, drift monitoring, and CI/CD around it.

What it is unusually good at, relative to conventional model servers:

| Strength | Why it matters in the MDLC |
| --- | --- |
| **Arbitrary Python in the serving path** | Pre/post-processing, business rules, feature lookups, and multi-model ensembles live in the same deployable unit as the model. No sidecar transformer, no separate orchestration service. Removes the classic train/serve skew where preprocessing is reimplemented in the serving tier. |
| **Model composition via `DeploymentHandle`** | A pipeline of models is a graph of independently-scaled, independently-resourced deployments in one application. Each node scales on its own bottleneck. |
| **Heterogeneous and fractional resources** | CPU pre-processing replicas and GPU model replicas in one app; `num_gpus=0.25` packs small models onto one accelerator. Directly attacks GPU under-utilisation, the dominant inference cost line. |
| **First-class LLM support** | `ray.serve.llm` wraps vLLM and SGLang with OpenAI-compatible endpoints, tensor and pipeline parallelism, multi-LoRA multiplexing, prefix-cache-aware routing, and prefill/decode disaggregation. |
| **One runtime for training, batch, and online** | Ray Data, Ray Train, and Ray Serve share a cluster abstraction, so the offline-scoring code used in validation and the online code used in production can be the same Python. |

The main costs: **you own a Ray cluster**, since the control plane is Ray actors rather than Kubernetes primitives; the debugging surface is distributed Python; and the release and rollback story depends on KubeRay maturity rather than on the Deployments and ReplicaSets your platform team already understands.

---

## 2. What Ray Serve is, precisely

- **A library, not a service.** `pip install "ray[serve]"`. You write Python classes, decorate them, and run them on a Ray cluster.
- **Deployment:** the unit of scaling. A decorated class or function, replicated N times as Ray actors, each with its own resource request.
- **Application:** a graph of deployments with one **ingress** deployment, bound to a route prefix. The unit of deploy, upgrade, and version.
- **Replica:** one actor executing user code. Maintains its own request queue; `async def` handlers give concurrency within a replica.
- **Proxy:** HTTP, and optionally gRPC, entry point. One per node when `proxy_location: EveryNode`.
- **Controller:** one global actor owning the control plane. Creates, updates, and destroys replicas and proxies, runs the autoscaler, and checkpoints state to the Ray GCS.

### 2.1 Request lifecycle

1. Request hits a proxy (Uvicorn-based by default; HAProxy from 2.56, see [§3.5](#35-llm-serving-specifics)).
2. Proxy resolves route prefix to the application ingress deployment.
3. Router selects a replica using **power-of-two-choices**, respecting `max_ongoing_requests`. If all replicas are at their limit, the request queues at the router.
4. Replica executes. Optionally `@serve.batch` coalesces concurrent calls into one vectorised forward pass.
5. Ingress may call downstream deployments via `DeploymentHandle`, using the same routing and batching logic, but **bypassing the external proxy** through in-cluster actor calls, or a gRPC data plane when throughput mode is on.
6. Response returns, streaming token by token if the handler is a generator.

### 2.2 Fault tolerance, honestly stated

| Failure | Behaviour |
| --- | --- |
| Replica actor dies | Controller replaces it. |
| Proxy actor dies | Controller restarts it. |
| Controller dies | Ray restarts it; state recovered from GCS checkpoint. |
| In-flight requests during any of the above | **Lost.** Transient state such as sockets and partial responses is not recovered. |
| Head node or whole cluster dies | **Not recoverable by Ray Serve alone.** This is the argument for KubeRay: the `RayService` CR is what rebuilds the cluster and re-deploys the app. |

That last row is why running Ray Serve in production without Kubernetes is a decision to make only for a low-tier workload.

---

## 3. MDLC mapping

Using an enterprise MDLC framing (development, independent validation, approval, deployment, ongoing monitoring, retirement), here is where Ray Serve is load-bearing, incidental, or absent.

| MDLC stage | Ray Serve role | What you must supply |
| --- | --- | --- |
| 1. Problem framing, feasibility | **None** | n/a |
| 2. Data and feature engineering | Incidental (Ray Data is a sibling, not Serve) | Feature store or pipeline |
| 3. Model development and training | Incidental (Ray Train) | Experiment tracking |
| 4. **Independent validation** | **Supporting.** Stand up the *candidate* behind an endpoint, or score a validation set through the exact serving code path | Validation harness, challenger comparison, evidence capture |
| 5. Approval and documentation | **None** | Model risk documentation, sign-off workflow |
| 6. **Packaging and versioning** | **Partial.** `serve build` config, container image, `runtime_env` | Model registry as source of truth; image build pipeline |
| 7. **Deployment and release** | **Core.** `RayService` CR, canary and incremental upgrade, blue-green | GitOps, approval gates, promotion pipeline |
| 8. **Serving and runtime** | **Core.** Autoscaling, batching, multiplexing, composition, LLM engines | Ingress, authn/z, rate limiting, quotas |
| 9. **Monitoring** | **Partial.** Rich operational telemetry, no model-quality telemetry | Drift and performance monitoring, ground-truth join, alerting on model metrics |
| 10. Retraining and challenger promotion | **Supporting.** Shadow and A/B traffic patterns | Orchestrator, decision rules |
| 11. Retirement | **Partial.** Delete app, or scale to zero | Decommissioning evidence, archive |

The load-bearing stages are **6 through 10**. Everything below drills into those.

---

### 3.1 Stage 4: independent validation

The property that matters here: **the serving code path can be exercised offline**. `serve.run()` returns a `DeploymentHandle`, so a validator can drive the *deployed graph* in-process without HTTP:

```python
import ray
from ray import serve
from my_app import build_app

handle = serve.run(build_app(model_uri="registry://fraud-scorer/17"), blocking=False)

# Same code path production traffic takes: preprocessing, composition, postprocessing.
results = ray.get([handle.remote(row) for row in validation_frame])
```

Why this is worth writing down in a validation report: it eliminates the "the validator tested the pickle, production runs a different preprocessing branch" finding. The artefact under validation is the *application*, not the model file.

For validation-set scoring at volume, drive the same handle from Ray Data so the scoring job scales across the cluster, or use Ray Data's own batch-inference path if you do not need the serving wrapper.

**Latency and throughput evidence** for the approval pack should be generated against a cluster shaped like production, meaning the same accelerator SKU, replica count, and `max_ongoing_requests`. It should report p50/p95/p99, plus TTFT and inter-token latency for LLMs. Ray Serve exposes these as Prometheus histograms ([§3.7](#37-stage-9-monitoring)), so the evidence is a Grafana export rather than a bespoke load-test harness.

---

### 3.2 Stage 6: packaging and versioning

Three artefacts define a deployable version. Treat all three as versioned inputs, and record all three in the model risk documentation.

**(a) The container image.** Python, CUDA, framework, and app code. Baked, tagged, immutable.

**(b) The Serve config** (`serve build my_module:app -o serve_config.yaml`):

```yaml
proxy_location: EveryNode
http_options:
  host: 0.0.0.0
  port: 8000
applications:
  - name: fraud-scorer
    route_prefix: /fraud
    import_path: fraud.app:app
    runtime_env:
      pip: ["torch==2.6.0", "transformers==4.51.0"]
      env_vars:
        MODEL_URI: "s3://models/fraud-scorer/17/"
    deployments:
      - name: Preprocessor
        num_replicas: 4
        ray_actor_options: {num_cpus: 1}
      - name: Scorer
        num_replicas: auto
        max_ongoing_requests: 8
        autoscaling_config:
          min_replicas: 2
          max_replicas: 20
          target_ongoing_requests: 4
        ray_actor_options: {num_gpus: 0.5}
        user_config:
          decision_threshold: 0.82
```

**(c) The model weights.** Resolved at replica startup from the registry: MLflow, an S3/GCS/Blob URI, or a registry-backed sidecar download. **Do not bake weights into the image** unless the model is small and the image is per-version. You lose the ability to promote a validated weight set independently of the code, and image size hurts cold start.

Two versioning mechanics worth knowing:

- **`user_config` is hot-reloadable.** Change `decision_threshold` and Serve calls `reconfigure()` on each replica without restarting it. Powerful for threshold tuning, and a governance hazard, because it changes model behaviour without an image change. Put `user_config` under the same change control as code, or you have an unlogged path to altering a validated decision boundary.
- **`runtime_env` installs dependencies at replica start.** Convenient in development, a reliability and reproducibility risk in production, since it adds a network dependency at scale-up time and invites resolution drift. **Pin everything, and prefer baked images for production tiers.**

---

### 3.3 Stage 7: deployment and release on a cloud platform

The supported production path is **KubeRay's `RayService` CRD**, which owns health checking, status reporting, failure recovery, and upgrades.

```yaml
apiVersion: ray.io/v1
kind: RayService
metadata:
  name: fraud-scorer
spec:
  serveConfigV2: |
    applications:
      - name: fraud-scorer
        import_path: fraud.app:app
        route_prefix: /fraud
        deployments: [...]
  rayClusterConfig:
    rayVersion: "2.57.0"
    headGroupSpec:
      rayStartParams: {num-cpus: "0"}       # keep app work off the head node
      template:
        spec:
          containers:
            - name: ray-head
              image: registry.example.com/fraud-scorer:1.4.2
    workerGroupSpecs:
      - groupName: gpu-workers
        replicas: 2
        minReplicas: 2
        maxReplicas: 20
        template:
          spec:
            containers:
              - name: ray-worker
                image: registry.example.com/fraud-scorer:1.4.2
                resources:
                  limits: {nvidia.com/gpu: 1}
```

**`rayStartParams: {num-cpus: "0"}` on the head group is a production must.** It advertises zero CPU on the head node so no replicas are scheduled there, leaving the controller and GCS uncontended. A head node fighting replicas for CPU is one of the most common causes of mysterious control-plane flakiness.

#### Release strategies

| Strategy | Mechanism | Cost | Notes |
| --- | --- | --- | --- |
| **In-place update** | Change `serveConfigV2` only, `kubectl apply` | None | Serve reconciles deployments in place. Fine for replica counts, `user_config`, autoscaling params. |
| **New-cluster (blue-green)** | Change anything in `rayClusterConfig` (image, resources) | **~200% of cluster compute** during cutover | Long the only zero-downtime option. Brutal for GPU fleets, since you briefly need double the accelerators. |
| **Incremental upgrade** | `NewClusterWithIncrementalUpgrade` | Configurable overhead | **Alpha in KubeRay v1.5.1**, behind the `RayServiceIncrementalUpgrade` feature gate. New cluster starts at 0% target capacity; KubeRay provisions a Gateway and HTTPRoute at 100% old / 0% new, then shifts traffic progressively, scaling each cluster as it goes. |

`NewCluster` remains the **default** `upgradeStrategy.type`. Options are `NewCluster`, `None`, and `NewClusterWithIncrementalUpgrade`.

Incremental upgrade configuration, where all four prerequisites are mandatory and the last one surprises people:

```yaml
spec:
  upgradeStrategy:
    type: NewClusterWithIncrementalUpgrade
    clusterUpgradeOptions:
      gatewayClassName: istio        # required
      stepSizePercent: 10            # required: traffic shifted per interval
      intervalSeconds: 60            # required: seconds between shifts
      maxSurgePercent: 100           # default 100: capacity added per step
```

Prerequisites: **(1)** Gateway API CRDs, with docs referencing `kubernetes-sigs/gateway-api` v1.4.0; **(2)** a Gateway API controller such as Istio, Contour, or GKE's, though docs say it is *primarily tested with Istio*; **(3)** a `GatewayClass` resource created by a cluster admin; **(4)** **the Ray Autoscaler must be enabled in the `RayCluster` spec.** Status surfaces `targetCapacity`, `trafficRoutedPercent`, and `lastTrafficMigratedTime` on both `activeServiceStatus` and `pendingServiceStatus`.

Enable the gate at operator install:

```bash
helm install kuberay-operator kuberay/kuberay-operator --version v1.6.0 \
  --set featureGates[0].name=RayServiceIncrementalUpgrade \
  --set featureGates[0].enabled=true
```

The incremental path is what makes Ray Serve economically viable for large GPU deployments, but note the maturity label *and* the Istio-shaped prerequisite. **Plan for blue-green as the fallback, and size accelerator quota accordingly** *(judgment)*, because betting on incremental upgrades means betting on an alpha feature gate plus a service mesh you may not run.

For in-place updates, `rolling_update_percentage` (default `0.2`) controls what fraction of replicas Serve replaces at a time.

#### Canary, A/B, and shadow

Ray Serve has no built-in traffic-splitting primitive. Three approaches:

1. **At the ingress** (Gateway API, Istio, or a cloud L7 load balancer), splitting across two `RayService` objects. Cleanest separation, standard tooling, and the platform team already knows it.
2. **Inside the application:** an ingress deployment holding handles to both champion and challenger, splitting by hash of a stable key. Gives per-request logging of both arms in one place, which is exactly what a challenger evaluation needs. Costs you a code path that must itself be validated.
3. **Shadow:** ingress fires the challenger call without awaiting it and returns the champion's response. A cheap way to collect challenger predictions on live traffic with no customer exposure. Watch that the fire-and-forget call cannot backpressure the champion path.

```python
@serve.deployment
class Router:
    def __init__(self, champion, challenger, shadow_only: bool = True):
        self.champion, self.challenger = champion, challenger
        self.shadow_only = shadow_only

    async def __call__(self, req):
        payload = await req.json()
        champ = self.champion.remote(payload)
        chal = self.challenger.remote(payload)   # not awaited on the critical path
        result = await champ
        asyncio.create_task(self._log_challenger(payload, chal))
        return result
```

---

### 3.4 Stage 8: the serving runtime capability set

#### Model composition

The differentiator. Deployments call each other through `DeploymentHandle`, and each scales and is resourced independently.

```python
@serve.deployment(ray_actor_options={"num_cpus": 1}, num_replicas=8)
class Embedder: ...

@serve.deployment(ray_actor_options={"num_gpus": 1}, num_replicas="auto")
class Reranker: ...

@serve.deployment
@serve.ingress(fastapi_app)          # FastAPI for validation and OpenAPI
class Ingress:
    def __init__(self, embedder, reranker):
        self.embedder, self.reranker = embedder, reranker

    @fastapi_app.post("/search")
    async def search(self, q: Query):
        vec = await self.embedder.remote(q.text)
        candidates = await self.retrieve(vec)
        return await self.reranker.remote(q.text, candidates)

app = Ingress.bind(Embedder.bind(), Reranker.bind())
```

Contrast with the single-container model server. There, this pipeline is either three network services you operate separately, or one container where the CPU embedder and GPU reranker are forced to scale together.

#### Dynamic request batching

`@serve.batch` accumulates concurrent requests up to `max_batch_size` or `batch_wait_timeout_s`, whichever fires first, then calls your handler with a list. The classic throughput-for-latency trade, exposed as two knobs:

```python
@serve.deployment
class Scorer:
    @serve.batch(max_batch_size=32, batch_wait_timeout_s=0.01)
    async def score(self, inputs: list[dict]) -> list[float]:
        return self.model(np.stack([featurize(i) for i in inputs])).tolist()
```

Serve exposes batch-utilisation metrics. If mean batch size sits far below `max_batch_size`, your timeout is too short or your traffic too thin for the setting.

#### Model multiplexing: many models, shared replicas

For long-tail model fleets, such as per-tenant, per-region, or per-segment variants of one architecture. Replicas cache models with LRU eviction, and the router does affinity routing on the `serve_multiplexed_model_id` header.

```python
@serve.deployment
class MultiTenantScorer:
    @serve.multiplexed(max_num_models_per_replica=3)
    async def get_model(self, model_id: str):
        return torch.load(f"s3://models/{model_id}/model.pt", weights_only=False)

    async def __call__(self, request):
        model = await self.get_model(serve.get_multiplexed_model_id())
        return model(await request.json())
```

Routing detail worth knowing: the router waits up to `RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S` (default 1s) for a replica that already holds the model. If none is free it falls back to any replica, which loads on demand. **A missing header routes randomly**, so always fail closed on it at ingress rather than silently serving the wrong tenant's model.

This is the pattern that turns 400 customer-specific models from 400 always-on deployments into a handful of replicas. It is also the pattern with the sharpest governance edge: many models share a process, so isolation is logical, not physical. For regulated per-tenant models, confirm that is acceptable before designing around it.

#### Fractional GPUs

`num_gpus=0.25` lets four replicas share one accelerator. Ray does **not** enforce memory isolation. It is bookkeeping, not partitioning. Two replicas that each think they have 40% of an 80GB card will happily OOM each other. Use it for small models with known, bounded footprints, and use MIG or separate nodes when you need hard isolation.

#### Async inference (Ray 2.51+)

For work that outlives an HTTP request: batch summarisation, transcription, video, multi-step agents. Serve consumes from a message broker such as Redis or SQS via `@task_consumer` with `TaskProcessorConfig` and `@task_handler`, so the client submits a task and polls rather than holding a connection. This is the sanctioned answer to inference that takes four minutes against a load balancer that times out at 60 seconds, which previously meant a DIY queue.

#### Custom request routing (Ray 2.51+)

Subclass `RequestRouter`, override `choose_replicas()`, and attach via `request_router_config`. Enables cache affinity, session stickiness, latency-aware routing, and priority routing. The LLM stack ships a production implementation, described below.

#### Custom and external autoscaling (Ray 2.51 and 2.52 alpha)

`record_autoscaling_stats()` plus a policy function over `Dict[DeploymentId, AutoscalingContext]` lets you scale on GPU utilisation, external queue depth, or a schedule instead of request count. **External scaling** (2.52, alpha) exposes an HTTP API for a controller outside Serve to set replica counts, which is the hook for predictive scaling driven by your own forecasting.

---

### 3.5 LLM serving specifics

`pip install "ray[llm]"`. `ray.serve.llm` is a thin, engine-agnostic layer over vLLM and SGLang.

```python
from ray.serve.llm import LLMConfig, build_openai_app

cfg = LLMConfig(
    model_loading_config={"model_id": "qwen-32b", "model_source": "s3://models/qwen-32b/"},
    accelerator_type="H100",
    deployment_config={
        "autoscaling_config": {"min_replicas": 1, "max_replicas": 8,
                               "target_ongoing_requests": 32}
    },
    engine_kwargs={"tensor_parallel_size": 4, "max_model_len": 32768,
                   "enable_prefix_caching": True,
                   "enable_lora": True, "max_loras": 4},
    # max_num_adapters_per_replica must match engine_kwargs["max_loras"]
    lora_config={"dynamic_lora_loading_path": "s3://adapters/",
                 "max_num_adapters_per_replica": 4},
)

app = build_openai_app({"llm_configs": [cfg]})   # OpenAI-compatible at /v1
```

Capabilities that matter for cost and latency:

- **OpenAI-compatible API.** Clients, evals, and gateways that speak OpenAI work unchanged. Cheap insurance against serving-layer lock-in.
- **Tensor and pipeline parallelism**, multi-node, for models exceeding one host.
- **Multi-LoRA multiplexing.** Many fine-tunes on one base model's weights, with adapter-affinity routing and LRU eviction per replica via `max_num_adapters_per_replica`, which must equal `max_loras` in `engine_kwargs`. For an org with per-domain fine-tunes, this collapses N GPU fleets into one.
- **`PrefixCacheAffinityRouter`.** Routes on prefix-tree match to replicas likely holding the KV cache, balanced against queue length. Enabled via `request_router_config={"request_router_class": PrefixCacheAffinityRouter}`, with tunables `imbalanced_threshold`, `match_rate_threshold`, `do_eviction`, `eviction_threshold_chars`, `eviction_target_chars`, and `eviction_interval_secs`.
- **Prefill/decode disaggregation.** Separate replica pools for the compute-bound prefill and memory-bandwidth-bound decode phases, scaled independently. Real gains, real operational complexity. Adopt after you have saturated the simpler levers.
- **Data-parallel attention and expert parallelism** for MoE models.

**On the prefix-router numbers.** The Ray docs describe the mechanism (match prefixes when replicas are balanced, fall back to power-of-two-choices when imbalanced) but **publish no performance figure**. The numbers circulating come from an Anyscale blog *(vendor-benchmarked)*:

| | |
|---|---|
| **Claimed** | 60% lower TTFT, over 40% better end-to-end throughput, and separately over 2.5x input-token processing throughput |
| **Model** | DeepSeek-R1-Distill-Qwen-32B, tensor-parallel 4 |
| **Hardware** | 64 L4 GPUs (eight 8xL4 machines) |
| **Runtime** | **RayTurbo**, Anyscale's proprietary runtime, not open-source Ray |
| **Workload** | Synthetic `PrefixRepetitionDataset`: 512-token shared prefix, 128-token suffix, 128-token output, 32 concurrent per replica |

A synthetic dataset engineered for prefix repetition is close to best case, so expect real chat and RAG traffic to land below it. Still likely the highest-leverage single knob for shared-system-prompt workloads *(judgment)*.

**Version note that changes the performance conversation (Ray 2.56+).** The throughput-optimised path replaces Python-runtime proxy routing with **HAProxy**, streams tokens directly from replica to proxy while bypassing the ingress router on the return path, and uses a **v2 Ray executor backend for vLLM** that moves Ray out of the data plane.

*(Vendor-benchmarked.)* Google Cloud reports **up to 5x higher throughput and 8x lower latency**, explicitly "compared to previous Ray Serve configurations" and also against "a plain vLLM setup using the Ray executor", on GKE with A4 VMs (NVIDIA HGX B200), eight replicas, using **Gemma 4 E2B**.

**Read the model choice carefully.** The blog states it picked a small, efficient model *"to isolate bottlenecks introduced from orchestration and routing."* That is a serving-layer measurement, deliberately designed so the model is not the bottleneck. It does **not** imply 5x on a 70B model, where GPU compute dominates and proxy overhead is noise. What it legitimately tells you: the Ray Serve orchestration tax that used to be real has largely been engineered away.

If your team benchmarked Ray Serve LLM before 2.56 and concluded the Python proxy was a tax, **that measurement is stale.** Rerun it.

Enable via env vars on head and worker pods:

```yaml
env:
  - {name: RAY_SERVE_ENABLE_HA_PROXY,            value: "1"}
  - {name: RAY_SERVE_THROUGHPUT_OPTIMIZED,       value: "1"}   # direct gRPC between replicas
  - {name: RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING, value: "1"}
  - {name: VLLM_USE_RAY_V2_EXECUTOR_BACKEND,     value: "1"}
```

Docs state the gains apply best above roughly 50 RPS, more than 250 concurrent connections per replica, and bursty traffic. Below that, the defaults are fine and simpler.

---

### 3.6 Autoscaling and capacity: the parameters that decide your bill

Two tiers stack here. **Serve's autoscaler**, which manages replicas and runs inside the controller, sits on top of the **Ray autoscaler**, which manages nodes, which in turn sits on top of the **cloud or Kubernetes node autoscaler**. All three latencies add up on a cold scale-up: new replica, then new Ray node, then new VM, then image pull, then model load. For a large LLM image that is minutes, not seconds. Size `min_replicas` against that reality, not against steady-state cost.

`num_replicas="auto"` turns on autoscaling with defaults, including `max_replicas: 100`.

| Parameter | Default | What it does |
| --- | --- | --- |
| `target_ongoing_requests` | `2` | Steady-state setpoint: average in-flight requests per replica. **The main latency/cost dial.** Lower means more replicas and less queuing. *(Default changed from `1.0` in Ray 2.32.0.)* |
| `max_ongoing_requests` | `5` | Hard per-replica concurrency cap; excess queues at the proxy. Too low throttles throughput; too high causes imbalanced routing and p99 spikes during upscale. *(Default changed from `100` in Ray 2.32.0.)* |
| `min_replicas` | `1` | `0` enables scale-to-zero. |
| `max_replicas` | `1` | Your blast-radius and cost ceiling. Note the low default; `num_replicas="auto"` raises it to `100`. |
| `initial_replicas` | *(unset, falls back to `min_replicas`)* | Startup count; useful for warm-up. |
| `upscale_delay_s` | `30` | Wait before scaling up. |
| `downscale_delay_s` | `600` | Wait before scaling down. |
| `downscale_to_zero_delay_s` | **no documented default** | Separate delay for the 1 to 0 transition. The field is documented; a default is not stated. Set it explicitly if you scale to zero. |
| `upscaling_factor` | `1.0` | Gain on upscale decisions. |
| `downscaling_factor` | `1.0` | Gain on downscale decisions. |
| `metrics_interval_s` | `10` | Replica reporting frequency. |
| `look_back_period_s` | `30` | Aggregation window. Longer smooths, shorter reacts. |
| `aggregation_function` | `"mean"` | Also `"max"` (spike-reactive) and `"min"` (conservative). |

Related deployment-level options, which are not part of `autoscaling_config`: `max_queued_requests` (default `-1`, meaning **no limit**, so set it if you want the proxy to shed load rather than queue unboundedly), `graceful_shutdown_wait_loop_s` (`2`), `graceful_shutdown_timeout_s` (`20`), `health_check_period_s` (`10`), `health_check_timeout_s` (`30`), and `rolling_update_percentage` (`0.2`).

Documented env vars for autoscaler internals: `RAY_SERVE_CONTROL_LOOP_INTERVAL_S` (`0.1`), `RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER` (`false`), `RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S` (`10.0`), and `RAY_SERVE_RECORD_AUTOSCALING_STATS_TIMEOUT_S` (`10.0`).

**Tuning heuristics, which are judgment rather than documented recommendations.** The Ray docs give the fields and semantics but do *not* publish per-workload numeric recipes. Treat everything in this block as a starting hypothesis to benchmark:

- Keep `max_ongoing_requests` meaningfully above `target_ongoing_requests`, starting around 1.5 to 2x, so the setpoint is reachable before the cap throttles.
- For heavyweight models processing one request at a time, a low `target_ongoing_requests` near 1 keeps queuing off the critical path. For vLLM-backed LLM replicas, which batch internally, the useful setpoint is far higher, in the tens of concurrent requests, because the engine *wants* a full batch.
- `metrics_interval_s` should be at or below `upscale_delay_s`, or the autoscaler is deciding on stale data.
- Oscillating replica counts call for lower scaling factors and a `look_back_period_s` aligned to your delays. Burst latency calls for a lower `upscale_delay_s` and a higher `upscaling_factor`.

**On composition.** The docs' model-composition example uses `target_ongoing_requests: 20` on the Driver deployment, versus a much lower value on the model deployments, but the page presents this as an example, **not** as a stated general recommendation. The underlying logic is sound and worth adopting, since an I/O-bound orchestration layer sustains far more concurrency than a compute-bound model replica, but benchmark it rather than citing it as doctrine. Getting it backwards produces the pathology where the gateway scales out, the models stay pinned, and all you have built is a bigger queue.

---

### 3.7 Stage 9: monitoring

Ray Serve gives you strong **operational** telemetry and **zero model-quality** telemetry. Both halves need to be in the monitoring plan, and only one comes free.

**Comes free:**

- **Ray Dashboard** on port 8265, with a Serve view covering applications, deployments, replicas, and logs.
- **CLI:** `serve status` for health and replica states, `serve config` for goal state. Both belong in your readiness probes and runbooks.
- **Logs** at `/tmp/ray/session_latest/logs/serve/` plus stderr, via the `ray.serve` Python logger. Set `LoggingConfig(encoding="JSON")` for structured logs, rotate with `RAY_ROTATION_MAX_BYTES` and `RAY_ROTATION_BACKUP_COUNT`, disable access logs per deployment with `enable_access_log=False`, and propagate `X-Request-ID` for end-to-end correlation. Ship to Loki, CloudWatch, or Log Analytics with the usual DaemonSet collector.
- **Prometheus metrics** across proxy (requests, latency histograms, errors), router (queue length, routing delay), replica (processing latency, throughput, health checks), autoscaler (target replicas, policy time), and batching (batch size and utilisation). Latency histogram buckets are tunable via `RAY_SERVE_REQUEST_LATENCY_BUCKETS_MS`, so **set these to your SLO boundaries** or your p99 panel is interpolation fiction. Prebuilt Grafana dashboards ship for Serve and Serve LLM.
- **Custom metrics** via Ray's counter, gauge, and histogram API, auto-tagged with deployment and replica.
- **Event-loop monitoring**, which flags scheduling latency caused by blocking code in an async handler. Genuinely useful, since sync CPU work inside `async def` is the most common Ray Serve performance bug.
- **Memory profiling** with `RAY_SERVE_ENABLE_MEMORY_PROFILING=1` and memray, for the leak that shows up on day nine.

**You must build:**

- **Prediction logging:** inputs, outputs, model version, request ID, and latency, written to a durable store. Do this in the ingress deployment, asynchronously, never on the critical path. This is the substrate for everything below.
- **Drift:** input distribution against the validation baseline, and prediction distribution over time.
- **Performance against ground truth:** the delayed join, where the label arrives days or weeks later. Entirely outside Ray Serve.
- **Alerting on model metrics:** thresholds that trip a review, not just a pager.
- **Threshold-change audit:** every `user_config` reconfigure, since those change decisions without a deploy.

That gap is not a criticism of Ray Serve. It is the boundary the docs advertise. But an MDLC that assumes "we have Ray Serve, therefore we have model monitoring" will fail its first validation review.

---

## 4. Cloud reference architecture

Target: managed Kubernetes on any of the three hyperscalers, the KubeRay operator, and one `RayService` per model application.

```
                       ┌─────────────────────────────────────┐
  clients ──► WAF/CDN ─►│  Cloud L7 LB / Gateway API + Istio │  authn/z, rate limit, canary split
                       └──────────────┬──────────────────────┘
                                      │
             ┌────────────────────────▼──────────────────────────┐
             │  Managed Kubernetes (EKS / GKE / AKS)             │
             │  ┌────────────────────────────────────────────┐   │
             │  │ KubeRay operator  (own node pool, tainted) │   │
             │  └────────────────────────────────────────────┘   │
             │  ┌── RayService: fraud-scorer ────────────────┐    │
             │  │  head pod   (CPU pool, num-cpus=0)         │    │
             │  │    controller · GCS · dashboard · proxy    │    │
             │  │  worker group: cpu-preproc  (CPU pool)     │    │
             │  │  worker group: gpu-model    (GPU pool)     │    │
             │  └────────────────────────────────────────────┘    │
             │  ┌── RayService: rag-assistant ───────────────┐    │
             │  │  ... vLLM replicas, H100 pool             │    │
             │  └────────────────────────────────────────────┘    │
             └──────┬───────────────┬────────────────┬───────────┘
                    │               │                │
           object store        Prometheus/       model registry
        (weights, logs)      Grafana/Loki      (MLflow or equiv)
```

Design decisions to make explicitly:

| Decision | Recommendation | Why |
| --- | --- | --- |
| One big Ray cluster or one per app? | **One `RayService` per application**, or per tightly-coupled family | Blast radius, independent upgrade cadence, clean cost attribution, per-model quota. A shared cluster couples unrelated release schedules, and one bad replica can starve the shared GCS. |
| Head node placement | Dedicated CPU node pool, `num-cpus: "0"` | Documented: *"setting `num-cpus:"0"` for the Ray head pod will prevent Ray workloads with non-zero CPU requirements from being scheduled on the head."* The control plane must not contend with inference. |
| Pod-to-node ratio | One large Ray pod per Kubernetes node | Documented: *"It's ideal to size each Ray pod to take up the entire Kubernetes node."* Better object-store usage, less intra-cluster communication overhead. |
| GPU node pools | Separate pool per accelerator SKU, with taints and `accelerator_type` in config | Prevents a CPU deployment landing on an H100 node. |
| Node scaling | Cluster Autoscaler, **Karpenter** on EKS, or node auto-provisioning on GKE | Ray's autoscaler asks for nodes; something must supply them. Spot or preemptible for stateless replicas, on-demand for the head node. **Never put the head on spot.** |
| Ingress | Gateway API | Required by KubeRay incremental upgrades, and also where canary splitting, authn, and rate limiting belong. |
| Weights delivery | Object store plus a node-local cache, or a read-only shared volume | Model load dominates cold start. Caching on the node turns minutes into seconds on scale-up. |
| Config delivery | GitOps (Argo or Flux) on the `RayService` manifest | Makes the deployed version auditable, a hard requirement for stage 5 evidence. |

**Platform-specific notes**

- **GKE:** the most mature integration. Ray Operator add-on, TPU as well as GPU support, a published multi-cluster Ray Serve plus GKE Inference Gateway pattern, and the 2.56 throughput work was co-developed and benchmarked here. Anyscale's RayTurbo runtime is being integrated with GKE.
- **EKS:** KubeRay plus Karpenter is well-trodden, and AWS's Data-on-EKS blueprints include Ray patterns. No managed Ray add-on, so you own the operator lifecycle.
- **AKS:** ⚠️ **low confidence.** There are reports of managed Ray via Anyscale on Azure announced at Build 2026, but the source found here was secondary rather than Microsoft or Anyscale directly. **Verify before this influences any decision.** The baseline assumption should be self-managed KubeRay on AKS, the same as EKS.
- **Anyscale**, the managed Ray offering from the Ray creators, runs on your EKS, GKE, AKS, or self-hosted Kubernetes, and adds RayTurbo, a managed control plane, and operational tooling. This is the build-versus-buy axis.
- **Vertex AI and SageMaker** have Ray-on-platform offerings that lean toward training and tuning. For *serving*, the native managed endpoint is the more natural comparison.

---

## 5. Failure modes and operational gotchas

Ranked by how often they bite in practice.

1. **Blocking code in an `async def` handler.** Stalls the replica's event loop, tanks throughput, and looks like a model performance problem. Use the event-loop-latency metric to catch it, then push sync work to threads or make the handler sync so Serve manages concurrency.
2. **Model load on the critical path of scale-up.** Every new replica pays it. Mitigate with a node-local weight cache, baked images for small models, warm `min_replicas`, and a `downscale_to_zero_delay_s` that is not punitive.
3. **`max_ongoing_requests` mis-set.** Too low and throughput collapses with idle GPUs. Too high and you get routing imbalance and p99 blowouts. Benchmark it rather than guessing.
4. **Blue-green GPU cost shock.** A `rayClusterConfig` change means a second full cluster. Discovering this at promotion time, against a GPU quota, is a bad day. Either get the incremental-upgrade gate working in a lower environment first, or hold headroom.
5. **Head-node contention and GCS pressure.** Fixed by `num-cpus: "0"` and a dedicated pool. Under-provisioning the head node manifests as random control-plane timeouts.
6. **`runtime_env` in production.** Dependency install at replica start adds a network dependency on your scale-up path and a reproducibility hole. Bake images.
7. **In-flight request loss on *crash*, not on planned shutdown.** Planned replica removal during downscale or rolling update drains, using `graceful_shutdown_wait_loop_s` (default 2s) then `graceful_shutdown_timeout_s` (default 20s) before the actor is killed. Loss occurs when a replica *crashes*, or when a request outlives the graceful timeout. **If your p99 exceeds 20s, raise `graceful_shutdown_timeout_s` or every downscale event truncates live requests.** Pair with idempotent handlers and client retries with jitter.
8. **Fractional-GPU OOM.** Ray's accounting is advisory. Bound the footprints or use MIG.
9. **Missing multiplex header.** Random routing, wrong model, and silent. Validate at ingress.
10. **Version-skew debugging.** Ray, KubeRay, vLLM, CUDA, and driver versions are a tightly coupled matrix. Pin the whole set, upgrade deliberately, and keep a known-good tuple documented.

---

## 6. What Ray Serve does not give you

Worth stating plainly in any adoption proposal, because each line is a workstream someone must own:

- Model registry, lineage, or artefact governance.
- Model-quality monitoring: drift, ground-truth join, performance decay.
- Approval workflow, sign-off records, model risk documentation.
- Feature store, or online/offline feature consistency guarantees.
- Built-in authn/authz, quota, or per-tenant rate limiting. Do it at the gateway.
- Traffic splitting as a first-class primitive. Gateway or app-level code.
- Explainability, bias, or fairness tooling.
- A UI for non-engineers. The Ray Dashboard is an operator's tool.

Pair Ray Serve with a registry such as MLflow or a cloud-native equivalent, GitOps for deploys, Prometheus/Grafana/Loki for ops telemetry, a dedicated ML-monitoring layer for model quality, and your existing API gateway for the edge.

---

## 7. Suggested adoption path

| Phase | Goal | Exit criteria |
| --- | --- | --- |
| **0. Spike (1–2 wks)** | One real model, `serve run` locally, then KubeRay in a dev cluster | Endpoint serving; the team can read Ray logs and the dashboard |
| **1. One production workload** | The workload that most needs composition or GPU efficiency, not the easiest one | SLO met; Prometheus and Grafana wired; runbook written; rollback rehearsed |
| **2. Release engineering** | GitOps on `RayService`, image pipeline, promotion gates, canary at the gateway, incremental-upgrade gate tested in staging | A model version promoted dev to staging to prod with an audit trail and no manual `kubectl` |
| **3. Monitoring gap closed** | Prediction logging, drift, ground-truth join, model-metric alerting | Passes a mock validation review |
| **4. Scale out** | Multiplexing for the long tail, the LLM stack with the 2.56 throughput path, autoscaling tuned per workload | GPU utilisation and cost per 1k inferences trending the right way |

The fastest way to get this wrong: adopt Ray Serve for a fleet of trivial single-model endpoints, inherit a distributed-systems operational burden, and never use the composition and GPU-efficiency features that were the actual reason to be here.

---

## 8. Questions to answer before adopting

Answers to these change the decision more than any feature comparison will:

1. **Workload shape.** How many endpoints, single-model or compositional, and what share is LLM versus classical?
2. **Accelerators.** Do you own GPU capacity and reservations, or would you be buying managed inference per token? This is mostly a procurement fact, and it dominates the economics.
3. **Traffic profile.** Steady or bursty? Is scale-to-zero relevant? What are the latency SLOs?
4. **Regulatory posture.** Full model risk management with independent validation and documented sign-off, or lighter touch? Determines how much of [§6](#6-what-ray-serve-does-not-give-you) is mandatory.
5. **Team.** Is there a platform team that can own a Ray cluster, or does the ML team operate its own serving?
6. **Existing estate.** Already on Kubernetes? Already running KServe or vendor endpoints? Migration cost is a first-class input.
7. **Multi-tenancy isolation.** Is logical isolation via multiplexing and shared replicas acceptable, or is physical isolation required per tenant or model?

---

## 9. Scope and limitations

Every figure and default in this document was verified against primary sources on 2026-08-13: Ray and KubeRay documentation pages, package registries, and named vendor benchmarks. Where a claim rests on a vendor benchmark rather than documentation, the benchmark conditions are stated inline so you can judge how far it transfers. Where a recommendation is engineering judgment rather than documented guidance, it is marked *(judgment)*.

Two specific caveats worth repeating, because both are easy to over-read:

- **The Ray docs publish no per-workload numeric tuning recipes.** The autoscaling parameter table in [§3.6](#36-autoscaling-and-capacity-the-parameters-that-decide-your-bill) gives verified defaults and semantics. The tuning heuristics beneath it are judgment, and are starting hypotheses to benchmark rather than settled guidance.
- **Vendor benchmark conditions matter more than the headline numbers.** The GKE 5x/8x figures used a deliberately small model to isolate orchestration overhead. The prefix-routing figures ran on Anyscale's proprietary RayTurbo runtime against a synthetic prefix-repetition dataset. Both are informative; neither transfers unmodified to a large model on stock Ray.

**Nothing in this document has been executed.** Every claim is documentation-derived or vendor-published:

1. **No hands-on validation.** No cluster stood up, no model served, no number reproduced. The autoscaling heuristics in [§3.6](#36-autoscaling-and-capacity-the-parameters-that-decide-your-bill) are untested hypotheses.
2. **Cold-start latency is unquantified.** Repeatedly called the dominant scale-up cost, never measured. Needs a real image and real weights on a specific accelerator SKU.
3. **Cost modelling is qualitative.** Levers described, but no cost-per-1k-inferences model.
4. **Security and multi-tenancy get one table row.** Authn/z, network policy, secret handling for weight access, and per-tenant isolation need their own design. KubeRay v1.6.0 reportedly adds Ray token authentication via Kubernetes RBAC, which is worth investigating and is unverified here.
5. **Async inference is doc-summary only.** The async-inference reference page was not fetched; the `@task_consumer`, `TaskProcessorConfig`, and `@task_handler` API shape comes from an Anyscale blog rather than the API docs.
6. **Ray Serve without Kubernetes** is dismissed in one line and not seriously evaluated.

---

## 10. Sources

Fetched 2026-08-12, re-verified 2026-08-13.

**Ray and Ray Serve documentation**

1. **Ray Serve overview**. Ray docs. <https://docs.ray.io/en/latest/serve/index.html>
2. **Ray Serve architecture**. Ray docs. <https://docs.ray.io/en/latest/serve/architecture.html>
3. **Production guide**. Ray docs. <https://docs.ray.io/en/latest/serve/production-guide/index.html>
4. **Ray Serve autoscaling**. Ray docs. <https://docs.ray.io/en/latest/serve/autoscaling-guide.html>
5. **Advanced Ray Serve autoscaling**. Ray docs. <https://docs.ray.io/en/latest/serve/advanced-guides/advanced-autoscaling.html>
6. **Monitoring Ray Serve**. Ray docs. <https://docs.ray.io/en/latest/serve/monitoring.html>
7. **Model multiplexing**. Ray docs. <https://docs.ray.io/en/latest/serve/model-multiplexing.html>
8. **Serve fault tolerance**. Ray docs. <https://docs.ray.io/en/latest/serve/production-guide/fault-tolerance.html>
9. **`@serve.deployment` API reference**. Ray docs. <https://docs.ray.io/en/latest/serve/api/doc/ray.serve.deployment_decorator.html>

**Ray Serve LLM**

10. **Serving LLMs with Ray Serve LLM**. Ray docs. <https://docs.ray.io/en/latest/serve/llm/index.html>
11. **Ray Serve LLM architecture overview**. Ray docs. <https://docs.ray.io/en/latest/serve/llm/architecture/overview.html>
12. **Prefix-aware request routing**. Ray docs. <https://docs.ray.io/en/latest/serve/llm/user-guides/prefix-aware-routing.html>
13. **LLM request routing policies**. Ray docs. <https://docs.ray.io/en/latest/serve/llm/architecture/routing-policies.html>
14. **Multi-LoRA deployment**. Ray docs. <https://docs.ray.io/en/latest/serve/llm/user-guides/multi-lora.html>
15. **`ray.serve.llm.LoraConfig` API**. Ray docs. <https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.LoraConfig.html>

**KubeRay and Kubernetes**

16. **Deploy Ray Serve applications (RayService)**. Ray docs. <https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/rayservice.html>
17. **RayService zero-downtime incremental upgrades**. Ray docs. <https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/rayservice-incremental-upgrade.html>
18. **RayService troubleshooting**. Ray docs. <https://docs.ray.io/en/latest/cluster/kubernetes/troubleshooting/rayservice-troubleshooting.html>
19. **High-throughput Ray Serve with KubeRay**. Ray docs. <https://docs.ray.io/en/master/cluster/kubernetes/user-guides/kuberay-serve-high-throughput.html>
20. **RayCluster configuration guide**. Ray docs. <https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/config.html>
21. **Ray on Kubernetes**. Ray docs. <https://docs.ray.io/en/latest/cluster/kubernetes/index.html>
22. **KubeRay documentation**. KubeRay. <https://ray-project.github.io/kuberay/>
23. **KubeRay releases**. GitHub. <https://github.com/ray-project/kuberay/releases>

**Anyscale**

24. **Introducing KubeRay v1.5**. Anyscale blog. <https://www.anyscale.com/blog/kuberay-v1-5>
25. **Ray Serve: async inference, custom request routing, custom autoscaling**. Anyscale blog. <https://www.anyscale.com/blog/ray-serve-autoscaling-async-inference-custom-routing>
26. **Reduce LLM inference latency by 60% with custom request routing**. Anyscale blog. <https://www.anyscale.com/blog/ray-serve-faster-first-token-custom-routing>
27. **Ray Serve LLM: wide-EP and disaggregated serving with vLLM**. Anyscale blog. <https://www.anyscale.com/blog/ray-serve-llm-anyscale-apis-wide-ep-disaggregated-serving-vllm>
28. **AI agents on Ray Serve: single to multi-agent architecture**. Anyscale blog. <https://www.anyscale.com/blog/ai-agents-on-ray-serve-single-to-multi-agent-architecture>
29. **Benchmarking with Ray Serve LLM**. Anyscale docs. <https://docs.anyscale.com/llm/serving/benchmarking/benchmarking-guide>

**Cloud platforms**

30. **Improving Ray Serve LLM on GKE throughput and latency**. Google Cloud blog. <https://cloud.google.com/blog/products/containers-kubernetes/improving-ray-serve-llm-on-gke-throughput-latency>
31. **Ray on GKE: new features for AI scheduling and scaling**. Google Cloud blog. <https://cloud.google.com/blog/products/containers-kubernetes/ray-on-gke-new-features-for-ai-scheduling-and-scaling>
32. **Partnering with Anyscale to integrate RayTurbo with GKE**. Google Cloud blog. <https://cloud.google.com/blog/products/containers-kubernetes/partnering-with-anyscale-to-integrate-rayturbo-with-gke>

**Package registry**

33. **ray on PyPI**, for version and release-date verification. <https://pypi.org/project/ray/>

---

*Published as open research. Corrections and counter-evidence welcome via issues.*
