from datetime import datetime
from pathlib import Path
import pytest

@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    now = datetime.now()
    reports_dir = Path('reports', now.strftime('%Y%m%d'))
    reports_dir.mkdir(parents=True, exist_ok=True)
    report = reports_dir / f"BE_QE_TESTS_REPORT_{now.strftime('%H%M')}.html"
    config.option.htmlpath = report
    config.option.self_contained_html = True