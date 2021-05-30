from single_bet_type_analyzer import SingleBetTypeAnalyzer
from dask.distributed import Client, LocalCluster
import pandas as pd


class EnsembleBetAnalyzer:
    def __init__(self, cluster=None, live=True, offline=True, headless=True):
        if cluster is None:
            self.cluster = LocalCluster(processes=False)
        else:
            self.cluster = cluster
        self.client = Client(self.cluster)
        # futures = [
        #     self.client.submit(SingleBetTypeAnalyzer, 'calcio', '1x2', self.cluster),
        #     self.client.submit(SingleBetTypeAnalyzer, 'calcio', 'uo1.5', self.cluster),
        #     self.client.submit(SingleBetTypeAnalyzer, 'calcio', 'uo2.5', self.cluster),
        #     self.client.submit(SingleBetTypeAnalyzer, 'calcio', 'uo3.5', self.cluster),
        #     self.client.submit(SingleBetTypeAnalyzer, 'calcio', 'uo4.5', self.cluster),
        #     self.client.submit(SingleBetTypeAnalyzer, 'basket', '12', self.cluster),
        #     self.client.submit(SingleBetTypeAnalyzer, 'tennis', '12', self.cluster),
        # ]
        # self.analyzers = [f.result() for f in futures]

        self.analyzers = [
            SingleBetTypeAnalyzer('calcio', '1x2', self.cluster, live=live, offline=offline, headless=headless),
            # SingleBetTypeAnalyzer('calcio', 'uo1.5', self.cluster, live=live, offline=offline, headless=headless),
            SingleBetTypeAnalyzer('calcio', 'uo2.5', self.cluster, live=live, offline=offline, headless=headless),
            # SingleBetTypeAnalyzer('calcio', 'uo3.5', self.cluster, live=live, offline=offline, headless=headless),
            # SingleBetTypeAnalyzer('calcio', 'uo4.5', self.cluster, live=live, offline=offline, headless=headless),
            # SingleBetTypeAnalyzer('basket', '12', self.cluster, live=live, offline=offline, headless=headless),
            SingleBetTypeAnalyzer('tennis', '12', self.cluster, live=live, offline=offline, headless=headless),
        ]

    def close(self):
        [analyzer.close() for analyzer in self.analyzers]
        self.client.close()
        self.cluster.close()

    def analyze_bets(self):
        results = [analyzer.analyze_bets() for analyzer in self.analyzers]
        df = pd.concat(results)
        return df.sort_values('ExpectedROI', ascending=False)


