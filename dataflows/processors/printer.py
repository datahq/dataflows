from datapackage import Package
from .. import DataStreamProcessor


class printer(DataStreamProcessor):

    def process_datapackage(self, dp: Package):
        print(dp.descriptor)
        return dp

    # def process_row(self, row):
    #     print(row)
    #     return row
