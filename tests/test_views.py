import yaml

def test_views_config_parses():
    """Ensure refined views.yaml is valid YAML."""
    with open("configs/refined/views.yaml", "r") as f:
        views = yaml.safe_load(f)
    assert isinstance(views, dict)
    assert "views" in views
    for v in views["views"]:
        assert "name" in v
        assert "query" in v
