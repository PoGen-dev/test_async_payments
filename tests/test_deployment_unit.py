from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_api_host_port_is_configurable_without_changing_container_port():
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    environment = (ROOT / ".env.example").read_text(encoding="utf-8")

    assert '"127.0.0.1:${API_HOST_PORT:-8080}:8000"' in compose
    assert "API_HOST_PORT=8080" in environment
    assert '"127.0.0.1:8000:8000"' not in compose
    assert "http://127.0.0.1:8000/health/live" in compose


def test_consumer_waits_for_rabbitmq_amqp_listener():
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")

    assert (
        "rabbitmq-diagnostics -q check_port_listener 5672 && rabbitmq-diagnostics -q check_port_connectivity"
        in compose
    )
    assert "rabbitmq: {condition: service_healthy}" in compose


def test_readme_uses_configured_default_host_port():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "API_HOST_PORT=8080" in readme
    assert "http://localhost:8080/health/ready" in readme
    assert "http://localhost:8080/api/v1/payments" in readme
    assert "http://localhost:8000/" not in readme


def test_python_imports_project_sources_not_an_installed_duplicate():
    import app

    assert Path(app.__file__).resolve() == ROOT / "app" / "__init__.py"


def test_docker_test_runner_collects_coverage_from_the_project():
    dockerfile = (ROOT / "Dockerfile.test").read_text(encoding="utf-8")

    assert "pip install --no-cache-dir -e '.[test]'" in dockerfile
    assert 'CMD ["python", "-m", "pytest"' in dockerfile
    assert '"--cov=app"' in dockerfile
    assert '"--cov-branch"' in dockerfile
    assert '"--cov-fail-under=90"' in dockerfile
