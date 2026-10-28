# Section 8 scale-out result

Command:

```bash
.venv/bin/python -m benchmarks.scale_out_demo
```

Result:

```text
Initial nodes: node1, node2, node3, node4
Added node:    node5
Chunks:        1000
Moved:         186 (18.60%)
Traffic:       1838 bytes
SHA-256:       PASS
```

This is an actual in-memory measurement using 1,000 content-addressed chunks
and the production consistent-hash class. The demonstration copied only chunks
whose primary target changed, removed their old simulated placement, and
recomputed every SHA-256 afterward. It does not claim live Docker, EKS, network,
or persistent-volume movement.

