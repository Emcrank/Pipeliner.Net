using System.Text.Json;
using System.Text.Json.Serialization;

namespace Pipeliner.Net;

internal static class PipelineCheckpointJson
{
    public static JsonSerializerOptions DefaultSerializerOptions { get; } = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };
}
