from pathlib import Path

import yaml

from benchmarks.scale_out_demo import run_scale_out_demo


ROOT = Path(__file__).resolve().parents[1]


def load_documents(path: Path):
    return [doc for doc in yaml.safe_load_all(path.read_text()) if doc]


def test_service_specific_dockerfiles_exist_and_are_used() -> None:
    expected = {
        "node1": "Dockerfile.storage",
        "metadata": "Dockerfile.metadata",
        "gateway1": "Dockerfile.gateway",
        "maintenance-worker": "Dockerfile.worker",
        "ui": "Dockerfile.ui",
    }
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    for service, dockerfile in expected.items():
        assert (ROOT / dockerfile).is_file()
        build = compose["services"][service].get("build", {})
        if service == "node1":
            # node1 receives its build block through the YAML storage anchor.
            assert compose["x-storage-node"]["build"]["dockerfile"] == dockerfile
        else:
            assert build["dockerfile"] == dockerfile


def test_aws_manifests_cover_required_workloads_and_health() -> None:
    docs = [doc for path in (ROOT / "k8s/aws").glob("*.yaml") for doc in load_documents(path)]
    deployments = {doc["metadata"]["name"]: doc for doc in docs if doc["kind"] == "Deployment"}
    assert {"gateway", "metadata", "maintenance-worker", "storage-ui"} <= deployments.keys()
    for name in ("gateway", "metadata", "storage-ui"):
        container = deployments[name]["spec"]["template"]["spec"]["containers"][0]
        assert "requests" in container["resources"] and "limits" in container["resources"]
        assert "readinessProbe" in container and "livenessProbe" in container
    statefulset = next(doc for doc in docs if doc["kind"] == "StatefulSet")
    assert statefulset["spec"]["volumeClaimTemplates"]


def test_gateway_hpa_scales_two_to_five_on_cpu() -> None:
    hpa = load_documents(ROOT / "k8s/aws/gateway-hpa.yaml")[0]
    assert hpa["apiVersion"] == "autoscaling/v2"
    assert hpa["spec"]["minReplicas"] == 2
    assert hpa["spec"]["maxReplicas"] == 5
    metric = hpa["spec"]["metrics"][0]["resource"]
    assert metric["name"] == "cpu"
    assert metric["target"]["averageUtilization"] == 70


def test_scale_out_moves_only_affected_chunks_and_preserves_integrity() -> None:
    result = run_scale_out_demo(1_000)
    assert result.moved_chunks == 186
    assert result.moved_percentage == 18.6
    assert result.repair_traffic_bytes > 0
    assert result.integrity_verified is True
