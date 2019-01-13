import os
import itertools
from dataflows import Flow
from .stream import stream
from .unstream import unstream


def _notify_checkpoint_saved(checkpoint_name):

    def step(package):
        yield package.pkg
        for rows in package:
            yield (row for row in rows)
        print(f"checkpoint saved: {checkpoint_name}")

    return step


class checkpoint(Flow):

    def __init__(self, checkpoint_name, checkpoint_path='.checkpoints', steps=None, resources=None):
        if not steps:
            steps = []
        super().__init__(*steps)
        self.checkpoint_name = checkpoint_name
        self.checkpoint_path = os.path.join(checkpoint_path, checkpoint_name)
        self.resources = resources

    def _preprocess_chain(self):
        checkpoint_package_json_path = os.path.join(self.checkpoint_path, 'stream.ndjson')
        if os.path.exists(checkpoint_package_json_path):
            print('using checkpoint data from {}'.format(self.checkpoint_path))
            return unstream(checkpoint_package_json_path),
        else:
            print('saving checkpoint to: {}'.format(self.checkpoint_path))
            return itertools.chain(self.chain, (stream(checkpoint_package_json_path),
                                                _notify_checkpoint_saved(self.checkpoint_name)))

    def handle_flow_checkpoint(self, parent_chain):
        self.chain = itertools.chain(self.chain, parent_chain)
        return [self]
