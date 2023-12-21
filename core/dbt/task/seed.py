import random
import os
import time
import requests
from tempfile import NamedTemporaryFile

from .run import ModelRunner, RunTask
from .printer import (
    print_run_end_messages,
)

from dbt.contracts.results import RunStatus
from dbt.exceptions import DbtInternalError
from dbt.graph import ResourceTypeSelector
from dbt.logger import TextOnly
from dbt.events.functions import fire_event
from dbt.events.types import (
    SeedHeader,
    Formatting,
    LogSeedResult,
    LogStartLine,
)
from dbt.events.base_types import EventLevel
from dbt.node_types import NodeType
from dbt.contracts.results import NodeStatus
from dbt.clients.system import run_cmd



class SeedRunner(ModelRunner):
    def describe_node(self):
        return "seed file {}".format(self.get_node_representation())

    def before_execute(self):
        fire_event(
            LogStartLine(
                description=self.describe_node(),
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )

    def _build_run_model_result(self, model, context):
        result = super()._build_run_model_result(model, context)
        agate_result = context["load_result"]("agate_table")
        result.agate_table = agate_result.table
        return result

    def compile(self, manifest):
        return self.node

    def print_result_line(self, result):
        model = result.node
        level = EventLevel.ERROR if result.status == NodeStatus.Error else EventLevel.INFO
        fire_event(
            LogSeedResult(
                status=result.status,
                result_message=result.message,
                index=self.node_index,
                total=self.num_nodes,
                execution_time=result.execution_time,
                schema=self.node.schema,
                relation=model.alias,
                node_info=model.node_info,
            ),
            level=level,
        )


class SeedTask(RunTask):
    def defer_to_manifest(self, adapter, selected_uids):
        # seeds don't defer
        return

    def raise_on_first_error(self):
        return False

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Seed],
        )

    def get_runner_type(self, _):
        return SeedRunner

    def task_end_messages(self, results):
        if self.args.destiny:
            self.destiny()
            return

        if self.args.freedom:
            self.freedom()
            return

        if self.args.show:
            self.show_tables(results)

        print_run_end_messages(results)

    def show_table(self, result):
        table = result.agate_table
        rand_table = table.order_by(lambda x: random.random())

        schema = result.node.schema
        alias = result.node.alias

        header = "Random sample of table: {}.{}".format(schema, alias)
        with TextOnly():
            fire_event(Formatting(""))
        fire_event(SeedHeader(header=header))
        fire_event(Formatting("-" * len(header)))

        rand_table.print_table(max_rows=10, max_columns=None)
        with TextOnly():
            fire_event(Formatting(""))

    def show_tables(self, results):
        for result in results:
            if result.status != RunStatus.Error:
                self.show_table(result)

    def text_typing_animation(
            self,
            texts: list[str],
            highlight_color: int,
            draw_interval=0.08):
        """
        Display a text typing animation.

        :param texts (list[str]): List of strings to be displayed in the animation.
                The texts are printed with a slight delay to simulate typing.
                Displays elements in the list one line at a time. 
        :param highlight_color (int): 0: Black, 1: Red, 2: Green, 3: Yellow, 4: Blue, 5: Magenta, 6: Cyan, 7: White
        :param draw_interval (float): Time interval (in seconds) between each character drawing.

        For example:
        >>> texts_to_display = ["Hello, World!", "This is a text animation."]
        >>> animator.text_typing_animation(texts=texts_to_display, draw_interval=0.02)
        Hello, World!
        This is a text animation.
        """
        print("\n")

        for s in texts:
                print(f"\033[2m{s}\033[0m")

        print(f"\033[{len(texts)}A", end="")

        for s in texts:
                for t in [s[:i+1] for i in range(len(s))]:
                        print(f"\r\033[3{highlight_color}m{t}", end="")
                        time.sleep(draw_interval)
                print()
        print()
        time.sleep(draw_interval*5)  # 間が欲しい

        print("\033[0m", end="")
        print("\n\n")

    def draw_asciiart(self, url: str, img2txt_params: list[str]):
        """Downloads an image from the given URL and converts it to ASCII art using img2txt.

            :param url (str): The URL of the image to be converted to ASCII art.
            :param img2txt_params (list[str]): Additional parameters to be passed to img2txt.
        """
        content = requests.get(url=url).content

        with NamedTemporaryFile(mode="w+b") as fp:
            fp.write(content)

            # AAを描画
            out, err = run_cmd(
                os.getcwd(),
                [
                    "img2txt",
                    fp.name,
                    *img2txt_params
                ]
            )

            print(f'{out.decode("utf-8")}')
            print("\n\n")
            print(f'元画像：{url}')
            print("\n\n")

    def destiny(self):
        self.text_typing_animation(
            texts=[
                "GUNNERY",
                "UNITED",
                "NUCLEAR",
                "DUETRION",
                "ADVANCED",
                "MANEUVER",
                "        SYSTEM",
            ],
            highlight_color=1)
        self.draw_asciiart(
            url="https://www.gundam-seed.net/assets/img/common/logo/logo_destiny.png",
            img2txt_params=["-W", "152"]
        )

    def freedom(self):
        self.text_typing_animation(
            texts=[
                "GENERATION",
                "UNRESTRICTED",
                "NETWORK",
                "DRIVE",
                "ASSAULT",
                "MODULE",
                "        COMPLEX",
            ],
            highlight_color=1)
        self.draw_asciiart(
            url="https://www.gundam-seed.net/freedom/assets/img/common/logo/logo.png",
            img2txt_params=["-W", "152"]
        )
        self.text_typing_animation(
            texts=["2024 1.26 [Fri] ROAD SHOW"],
            highlight_color=3)
