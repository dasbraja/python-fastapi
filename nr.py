from config import NR_ENV
import newrelic.agent as nr
from pathlib import Path

base_dir = Path(__file__).parent
nr.initialize(f'{base_dir}/newrelic.ini', NR_ENV)
