import subprocess

def test_init_remote():
    subprocess.check_output('cd ./out && dataflows init https://raw.githubusercontent.com/datahq/dataflows/master/data/academy.csv',
                            shell=True)
