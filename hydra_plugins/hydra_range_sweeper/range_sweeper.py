# Based on: https://github.com/BadrYoubiIdrissi/hydra-plugins/blob/master/badr_range_sweeper/hydra_plugins/range_sweeper_badr/range_sweeper_badr.py
import glob
import itertools
import logging
import re
from typing import Any, List, Optional

from omegaconf import DictConfig

from hydra.core.config_loader import ConfigLoader
from hydra.core.config_search_path import ConfigSearchPath
from hydra.core.plugins import Plugins
from hydra.plugins.launcher import Launcher
from hydra.plugins.search_path_plugin import SearchPathPlugin
from hydra.plugins.sweeper import Sweeper
from hydra.types import TaskFunction

log = logging.getLogger(__name__)


class RangeSweeperSearchPathPlugin(SearchPathPlugin):
    """
    This plugin is allowing configuration files provided by the RangeSweeper plugin to be discovered
    and used once the RangeSweeper plugin is installed
    """

    def manipulate_search_path(self, search_path: ConfigSearchPath) -> None:
        # Appends the search path for this plugin to the end of the search path
        search_path.append(
            "hydra-range-sweeper", "pkg://hydra_plugins.hydra_range_sweeper.conf"
        )


class RangeSweeper(Sweeper):
    def __init__(self):
        self.config: Optional[DictConfig] = None
        self.launcher: Optional[Launcher] = None
        self.job_results = None

    def setup(
        self,
        config: DictConfig,
        config_loader: ConfigLoader,
        task_function: TaskFunction,
    ) -> None:
        self.config = config
        self.launcher = Plugins.instantiate_launcher(
            config=config, config_loader=config_loader, task_function=task_function
        )

    def sweep(self, arguments: List[str]) -> Any:
        assert self.config is not None
        assert self.launcher is not None
        log.info("RangeSweeper")
        log.info("Sweep output dir : {}".format(self.config.hydra.sweep.dir))
        # Construct list of overrides per job we want to launch
        src_lists = []
        for s in arguments:
            key, value = s.split("=")
            gl = re.match(r"glob\((.+)\)", value)
            if "," in value:
                possible_values = value.split(",")
            elif ":" in value:
                possible_values = range(*[int(v) for v in value.split(":")])
            elif gl:
                possible_values = list(glob.glob(gl[1], recursive=True))
            else:
                possible_values = [value]
            src_lists.append(["{}={}".format(key, val) for val in possible_values])

        batch = list(itertools.product(*src_lists))

        returns = [self.launcher.launch(batch)]
        return returns
