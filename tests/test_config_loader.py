from config.config_loader import load_config

def test_load_config_reads_yaml(tmp_path):
    cfg_file = tmp_path / "ingestion.yaml"
    cfg_file.write_text(
        """
app:
  name: test_app
  log_level: INFO
""",
        encoding="utf-8",
    )

    cfg = load_config(str(cfg_file))
    assert cfg["app"]["name"] == "test_app"
    assert cfg["app"]["log_level"] == "INFO"