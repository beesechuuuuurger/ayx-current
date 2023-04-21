# Copyright (C) 2022 Alteryx, Inc. All rights reserved.
#
# Licensed under the ALTERYX SDK AND API LICENSE AGREEMENT;
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.alteryx.com/alteryx-sdk-and-api-license-agreement
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Example pass through tool."""
from typing import TYPE_CHECKING

from ayx_python_sdk.core import (
    Anchor,
    PluginV2,
)
from ayx_python_sdk.providers.amp_provider.amp_provider_v2 import AMPProviderV2

if TYPE_CHECKING:
    import pyarrow as pa


class AyxGPT(PluginV2):
    """A sample Plugin that passes data from an input connection to an output connection."""

    def __init__(self, provider: AMPProviderV2):
        """Construct the plugin."""
        self.name = "AyxGPT"
        self.provider = provider
        self.provider.io.info(f"{self.name} tool started")

    def on_record_batch(self, batch: "pa.Table", anchor: Anchor) -> None:
        """
        Process the passed record batch.

        The method that gets called whenever the plugin receives a record batch on an input.

        This method IS NOT called during update-only mode.

        Parameters
        ----------
        batch
            A pyarrow Table containing the received batch.
        anchor
            A namedtuple('Anchor', ['name', 'connection']) containing input connection identifiers.
        """
        self.provider.write_to_anchor("Output", batch)

    def on_incoming_connection_complete(self, anchor: Anchor) -> None:
        """
        Call when an incoming connection is done sending data including when no data is sent on an optional input anchor.

        This method IS NOT called during update-only mode.

        Parameters
        ----------
        anchor
            NamedTuple containing anchor.name and anchor.connection.
        """
        self.provider.io.info(
            f"Received complete update from {anchor.name}:{anchor.connection}."
        )

    def on_complete(self) -> None:
        """
        Clean up any plugin resources, or push records for an input tool.

        This method gets called when all other plugin processing is complete.

        In this method, a Plugin designer should perform any cleanup for their plugin.
        However, if the plugin is an input-type tool (it has no incoming connections),
        processing (record generation) should occur here.

        Note: A tool with an optional input anchor and no incoming connections should
        also write any records to output anchors here.
        """
        self.provider.io.info(f"{self.name} tool done.")
