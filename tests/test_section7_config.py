from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def documents(path: str):
    return list(yaml.safe_load_all((ROOT / path).read_text()))


def test_compose_defines_section7_services() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    services = compose["services"]
    assert {"gateway1", "gateway2", "nginx", "rabbitmq", "maintenance-worker",
            "prometheus", "grafana"} <= set(services)
    assert services["nginx"]["ports"] == ["8080:8080"]
    assert "maintenance" in (ROOT / "config/rabbitmq-definitions.json").read_text()


def test_storage_statefulset_has_persistence_and_probes() -> None:
    statefulset = next(doc for doc in documents("k8s/storage-statefulset.yaml")
                       if doc["kind"] == "StatefulSet")
    assert statefulset["spec"]["replicas"] == 6
    assert statefulset["spec"]["volumeClaimTemplates"][0]["metadata"]["name"] == "chunk-data"
    container = statefulset["spec"]["template"]["spec"]["containers"][0]
    assert container["readinessProbe"]["httpGet"]["path"] == "/health"
    assert container["livenessProbe"]["httpGet"]["path"] == "/health"


def test_etcd_template_has_three_persistent_members() -> None:
    statefulset = next(doc for doc in documents("k8s/etcd-ha-template.yaml")
                       if doc["kind"] == "StatefulSet")
    assert statefulset["spec"]["replicas"] == 3
    assert statefulset["spec"]["volumeClaimTemplates"][0]["metadata"]["name"] == "etcd-data"


def test_monitoring_and_nginx_configs_reference_expected_targets() -> None:
    nginx = (ROOT / "config/nginx.conf").read_text()
    prometheus = yaml.safe_load((ROOT / "config/prometheus.yml").read_text())
    assert "gateway1:8080" in nginx and "gateway2:8080" in nginx
    targets = str(prometheus)
    assert "gateway1:8080" in targets and "metadata:9000" in targets and "node6:8000" in targets
