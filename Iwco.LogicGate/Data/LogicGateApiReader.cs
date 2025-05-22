using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Iwco.LogicGate.Data
{
    public class LogicGateApiReader
    {
        private readonly string _bearerToken;
        private readonly HttpClient _client;

        public LogicGateApiReader(string bearerToken)
        {
            _bearerToken = bearerToken;

            
            _client = new HttpClient();
            _client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", bearerToken.Replace("Bearer ", ""));
        }

        public async Task<JsonArray> GetAllRecordsAsync(string workflowId, string stepId)
        {
            var allRecords = new JsonArray();
            int page = 0;
            int size = 100;
            int totalPages = int.MaxValue; // Will update this after the first call

            while (page < totalPages)
            {
                var url = $"https://iwco.logicgate.com/api/v2/records?workflow-id={workflowId}&step-id={stepId}&page={page}&size={size}";
                Console.WriteLine($"Fetching page {page}...");

                HttpResponseMessage response = await _client.GetAsync(url);
                response.EnsureSuccessStatusCode();

                var contentString = await response.Content.ReadAsStringAsync();
                var root = JsonSerializer.Deserialize<JsonObject>(contentString);

                if (root?["content"] is not JsonArray pageContent || pageContent.Count == 0)
                    break;

                foreach (var record in pageContent)
                {
                    if (record is JsonObject obj)
                    {
                        allRecords.Add(JsonNode.Parse(obj.ToJsonString())!);
                    }
                }

                // Update totalPages from "page" section in root
                if (root["page"] is JsonObject pageInfo &&
                    pageInfo["totalPages"] is JsonNode totalPagesNode &&
                    int.TryParse(totalPagesNode.ToString(), out var pages))
                {
                    totalPages = pages;
                }

                page++;
            }

            return allRecords;
        }

    }
}
