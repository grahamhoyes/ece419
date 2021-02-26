import subprocess as sp
import os
import csv

import tqdm

os.chdir("../..")

sp.run(["ant", "build-server-jar"])
sp.run(["ant", "build-performancetest-jar"])
data = [["num_servers", "num_clients", "ratio", "num_writes", "num_reads", "time_taken", "time_to_add_a_node", "time_to_remove_a_node"]]

numServersToTest = [2, 5, 8, 10]
numClientsToTest = [2, 5, 8, 10]
ratiosToTest = [0.2, 0.5, 0.8]

for numServers in tqdm.tqdm(numServersToTest):
    for numClients in numClientsToTest:
        for ratio in ratiosToTest:
            proc = sp.run(
                [
                    "java",
                    "-jar",
                    "PerformanceTestM2.jar",
                    "ecs.config",
                    "127.0.0.1",
                    "2181",
                    "/home/daniil/uni/ece419/ece419/KVServer.jar",
                    "/home/daniil/uni/ece419/m2/maildir",
                    str(numServers),
                    str(numClients),
                    str(ratio),
                ],
                capture_output=True,
            )
            data.append(proc.stdout.decode().split("\n")[:-1])

with open("performance_data.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(data)

