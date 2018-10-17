import os
import itertools
from dataflows import Flow
from . import dump_to_path, load


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
        checkpoint_package_json_path = os.path.join(self.checkpoint_path, 'datapackage.json')
        if os.path.exists(checkpoint_package_json_path):
            print('using checkpoint data from {}'.format(self.checkpoint_path))
            return load(checkpoint_package_json_path, resources=self.resources),
        else:
            print('saving checkpoint to: {}'.format(self.checkpoint_path))
            return itertools.chain(self.chain, (dump_to_path(self.checkpoint_path, resources=self.resources),
                                                _notify_checkpoint_saved(self.checkpoint_name)))

    def handle_flow_checkpoint(self, parent_chain):
        self.chain = itertools.chain(self.chain, parent_chain)
        return [self]
