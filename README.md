# Ray Serve for Inference in the MDLC

Where Ray Serve fits, and does not fit, in an enterprise Model Development Life Cycle, for teams deploying on a cloud platform.

**📄 [Read the full report](https://byte-ish.github.io/ray-serve-inference-mdlc/)** · [Markdown source](REPORT.md)

| | |
|---|---|
| **Scope** | Ray Serve as an online inference layer, mapped stage by stage onto the MDLC. Cloud deployment via managed Kubernetes. |
| **Verdict** | Strongest fit for *compositional* inference (multi-model pipelines, RAG, agentic flows) and LLM serving on self-owned accelerators. Over-engineered for a fleet of single-model, tensor-in/tensor-out endpoints. |
| **Confidence** | Moderate. Every claim is documentation-derived or vendor-published. **Nothing here has been executed:** no cluster stood up, no benchmark reproduced. |
| **Versions** | Ray **2.57.0** (latest stable on PyPI, released 2026-08-11), KubeRay **v1.6.2** |
| **Researched** | 2026-08-12, fact-checked 2026-08-13 |
| **Shelf life** | Short. Several load-bearing capabilities landed in Ray 2.51 to 2.56. Re-verify anything version-gated after roughly two quarters. |

## Key takeaways

1. **Ray Serve owns serving, not the lifecycle.** The Ray docs state plainly that it is not a full ML platform and lacks model lifecycle management and performance visualisation. Registry, lineage, validation evidence, drift monitoring, and CI/CD are all yours to supply.
2. **The differentiator is composition, not raw serving throughput.** Arbitrary Python in the request path plus independently-scaled deployments joined by `DeploymentHandle` means a pipeline is one deployable unit where each node scales on its own bottleneck. If your workload is not compositional, most of the reason to accept Ray's operational cost goes unused.
3. **Upgrade economics are the sharpest adoption risk.** Changing anything in `rayClusterConfig`, including the container image, triggers a blue-green needing roughly 200% of cluster compute. The efficient alternative is alpha in KubeRay v1.5.1 behind a feature gate, and needs Gateway API plus a controller the docs say is primarily tested with Istio.
4. **Ray 2.56 invalidated older benchmarks.** HAProxy routing, direct token streaming, and a v2 Ray executor for vLLM. If your team measured a Ray Serve orchestration tax before 2.56, that measurement is stale.
5. **Two governance edges are specific to Ray Serve.** `user_config` hot-reloads without restarting replicas, so a validated threshold can change with no image change and no deploy record. And model multiplexing shares one process across many models, making tenant isolation logical rather than physical.

## Where Ray Serve lands in the lifecycle

| MDLC stage | Ray Serve role |
|---|---|
| Problem framing, data, training | **None to incidental** |
| Independent validation | **Supporting** |
| Approval and documentation | **None** |
| Packaging and versioning | **Partial** |
| Deployment and release | **Core** |
| Serving and runtime | **Core** |
| Monitoring | **Partial** (operational only, no model quality) |
| Retraining and challenger promotion | **Supporting** |
| Retirement | **Partial** |

Load-bearing stages are packaging through challenger promotion. The report drills into each.

## Contents

| Section | What it covers |
|---|---|
| §2 | Architecture: controller, proxies, replicas, request lifecycle, fault tolerance |
| §3 | Stage-by-stage MDLC mapping, with config and code for each |
| §3.5 | LLM serving: vLLM/SGLang, multi-LoRA, prefix-cache routing, the 2.56 throughput path |
| §3.6 | Every autoscaling parameter with verified defaults, and what drives the bill |
| §4 | Cloud reference architecture on managed Kubernetes, with per-platform notes |
| §5 | Ten failure modes ranked by how often they bite |
| §6 | What you must build yourself |
| §9 | Scope and limitations: how far these claims can be pushed |

## How to read the evidence

Claims are labelled by provenance. **Documented** claims trace to a specific Ray or KubeRay page in the source list. **Vendor-benchmarked** claims come from Anyscale or cloud-provider blogs, and the benchmark conditions are stated inline so you can judge how far they transfer. **Judgment** marks engineering opinion rather than documented guidance, which matters most in the autoscaling tuning heuristics, since the Ray docs publish no per-workload numeric recipes.

Every figure and default was verified against primary sources on 2026-08-13. Nothing has been executed: no cluster stood up, no benchmark reproduced. [§9](https://byte-ish.github.io/ray-serve-inference-mdlc/#9-scope-and-limitations) states what that does and does not buy you.

## Rebuilding the site

```bash
python3 build.py REPORT.md docs/index.html \
  --title "Ray Serve for Inference in the MDLC" \
  --subtitle "Where Ray Serve fits, and does not fit, in an enterprise model development life cycle" \
  --eyebrow "Research & reference" \
  --badge "Ray 2.57.0" --badge "KubeRay v1.6.2" --badge "33 sources" --badge "Aug 2026" \
  --repo byte-ish/ray-serve-inference-mdlc
```

Requires pandoc. The generated `docs/index.html` is self-contained, with no external resource loads.

## Licence

[CC BY 4.0](LICENSE). Corrections and counter-evidence welcome via issues.
