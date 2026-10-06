// This file is subject to the terms and conditions defined
// in file 'LICENSE', which is part of this source code package.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using SteamKit2.CDN;

namespace DepotDownloader
{
    /// <summary>
    /// CDNClientPool provides a pool of connections to CDN endpoints, requesting CDN tokens as needed
    /// </summary>
    class CDNClientPool
    {
        private readonly Steam3Session steamSession;
        private readonly uint appId;
        public Client CDNClient { get; }
        public Server ProxyServer { get; private set; }

        private readonly List<Server> servers = [];
        private int nextServer;

        public CDNClientPool(Steam3Session steamSession, uint appId)
        {
            this.steamSession = steamSession;
            this.appId = appId;
            CDNClient = new Client(steamSession.steamClient);
        }

        public async Task UpdateServerList()
        {
            var servers = await this.steamSession.steamContent.GetServersForSteamPipe();

            ProxyServer = servers.Where(x => x.UseAsProxy).FirstOrDefault();

            var eligibleServers = servers
                .Where(server =>
                {
                    var isEligibleForApp = server.AllowedAppIds.Length == 0 || server.AllowedAppIds.Contains(appId);
                    return isEligibleForApp && (server.Type == "SteamCache" || server.Type == "CDN");
                })
                .ToList();

            if (eligibleServers.Count == 0)
            {
                throw new Exception("Failed to retrieve any download servers.");
            }

            if (ContentDownloader.Config.ProbeCDN)
            {
                await ProbeServersAsync(eligibleServers);
            }

            var weightedCdnServers = eligibleServers
                .Select(server =>
                {
                    AccountSettingsStore.Instance.ContentServerPenalty.TryGetValue(server.Host, out var penalty);
                    return (server, penalty);
                })
                .OrderBy(pair => pair.penalty)
                .ThenBy(pair => pair.server.WeightedLoad);

            this.servers.Clear();
            foreach (var (server, weight) in weightedCdnServers)
            {
                for (var i = 0; i < server.NumEntries; i++)
                {
                    this.servers.Add(server);
                }
            }
        }

        private static async Task ProbeServersAsync(List<Server> candidateServers)
        {
            var distinctCandidates = candidateServers
                .GroupBy(s => s.Host, StringComparer.OrdinalIgnoreCase)
                .Select(g => g.First())
                .Take(12)
                .ToList();

            if (distinctCandidates.Count == 0) return;

            Console.WriteLine("Probing {0} Steam CDN edge servers for lowest latency...", distinctCandidates.Count);

            using var http = HttpClientFactory.CreateHttpClient();
            var latencies = new ConcurrentDictionary<string, long>(StringComparer.OrdinalIgnoreCase);

            await Parallel.ForEachAsync(distinctCandidates,
                new ParallelOptions { MaxDegreeOfParallelism = Math.Min(6, distinctCandidates.Count) },
                async (server, ct) =>
                {
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
                        var protocol = server.Protocol == Server.ConnectionProtocol.HTTPS ? "https" : "http";
                        var requestUri = $"{protocol}://{server.Host}/";
                        using var req = new HttpRequestMessage(HttpMethod.Head, requestUri);
                        using var resp = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token);
                        sw.Stop();
                        latencies[server.Host] = sw.ElapsedMilliseconds;
                    }
                    catch
                    {
                        latencies[server.Host] = 99999;
                    }
                });

            foreach (var server in candidateServers)
            {
                if (latencies.TryGetValue(server.Host, out var ms))
                {
                    if (ms >= 99999)
                    {
                        // Unreachable or timed out during probe, penalize
                        AccountSettingsStore.Instance.ContentServerPenalty.AddOrUpdate(server.Host, 50, (k, old) => old + 50);
                    }
                    else
                    {
                        // Scale latency (ms / 10) into penalty points to prioritize responsive hosts
                        var latencyPenalty = (int)(ms / 10);
                        AccountSettingsStore.Instance.ContentServerPenalty.AddOrUpdate(server.Host, latencyPenalty, (k, old) => old + latencyPenalty);
                    }
                }
            }

            var best = latencies.OrderBy(kv => kv.Value).FirstOrDefault();
            if (best.Key != null && best.Value < 99999)
            {
                Console.WriteLine("Fastest CDN edge server: {0} ({1} ms)", best.Key, best.Value);
            }
        }

        public Server GetConnection()
        {
            return servers[nextServer % servers.Count];
        }

        public void ReturnConnection(Server server)
        {
            if (server == null) return;
        }

        public void ReturnBrokenConnection(Server server)
        {
            if (server == null) return;

            // Dynamically penalize failing server in ContentServerPenalty so it drops in future rankings
            AccountSettingsStore.Instance.ContentServerPenalty.AddOrUpdate(server.Host, 10, (k, old) => old + 10);

            lock (servers)
            {
                if (servers.Count > 0 && servers[nextServer % servers.Count] == server)
                {
                    nextServer++;
                }
            }
        }
    }
}
