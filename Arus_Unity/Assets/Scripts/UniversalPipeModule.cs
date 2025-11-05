using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using UnityEngine;

namespace UniversalPipeModule
{
    public class UniversalPipeModule : MonoBehaviour
    {
        [Header("Module Configuration")]
        [SerializeField] private string moduleName = "UnityModule";
        [SerializeField] private string modulePipeName = "unity_module"; // Pipe дл€ общени€ с оркестратором

        [Header("Settings")]
        [SerializeField] private bool autoStart = true;
        [SerializeField] private float healthCheckInterval = 10f;
        [SerializeField] private float connectionTimeout = 5f;

        private CancellationTokenSource _cancellationTokenSource;
        private bool _isRunning = false;
        private NamedPipeServerStream _serverPipe;

        public event Action<WorkItem> OnWorkItemReceived;
        public event Action<string> OnError;
        public event Action<bool> OnModuleStatusChanged;

        [System.Serializable]
        public class WorkItem
        {
            public string id;
            public string type;
            public string data; // base64 encoded
            public string createdAt;
            public Dictionary<string, string> metadata;

            public bool IsValid() => !string.IsNullOrWhiteSpace(type) && !string.IsNullOrEmpty(data);

            public byte[] ToBytes()
            {
                var json = JsonUtility.ToJson(this);
                return Encoding.UTF8.GetBytes(json);
            }

            public static WorkItem FromBytes(byte[] data)
            {
                try
                {
                    var json = Encoding.UTF8.GetString(data);
                    return JsonUtility.FromJson<WorkItem>(json);
                }
                catch (Exception ex)
                {
                    Debug.LogError($"Failed to deserialize WorkItem: {ex.Message}");
                    return new WorkItem();
                }
            }

            public string GetDataAsString()
            {
                try
                {
                    var bytes = Convert.FromBase64String(data);
                    return Encoding.UTF8.GetString(bytes);
                }
                catch
                {
                    return string.Empty;
                }
            }

            public T GetDataAsJson<T>()
            {
                var jsonString = GetDataAsString();
                return JsonUtility.FromJson<T>(jsonString);
            }

            public void SetDataFromString(string text)
            {
                data = Convert.ToBase64String(Encoding.UTF8.GetBytes(text));
            }

            public void SetDataFromJson<T>(T obj)
            {
                var json = JsonUtility.ToJson(obj);
                SetDataFromString(json);
            }
        }

        [System.Serializable]
        public class WorkResult
        {
            public string requestId;
            public bool success;
            public string data; // base64 encoded
            public string errorMessage;
            public string processedAt;

            public bool IsValid() => !string.IsNullOrWhiteSpace(requestId);

            public byte[] ToBytes()
            {
                var json = JsonUtility.ToJson(this);
                return Encoding.UTF8.GetBytes(json);
            }

            public WorkResult WithStringData(string text)
            {
                data = Convert.ToBase64String(Encoding.UTF8.GetBytes(text));
                return this;
            }

            public WorkResult WithJsonData<T>(T obj)
            {
                var json = JsonUtility.ToJson(obj);
                data = Convert.ToBase64String(Encoding.UTF8.GetBytes(json));
                return this;
            }
        }

        [System.Serializable]
        private class WorkItemWrapper
        {
            public string Type;
            public string Data;
            public Dictionary<string, string> Metadata;
        }

        [System.Serializable]
        private class WorkResultWrapper
        {
            public string RequestId;
            public bool Success;
            public string Data;
            public string ErrorMessage;
            public string ProcessedAt;
        }

        private void Start()
        {
            _cancellationTokenSource = new CancellationTokenSource();

            if (autoStart)
            {
                StartModule();
            }
        }

        public void StartModule()
        {
            if (_isRunning) return;

            _isRunning = true;
            OnModuleStatusChanged?.Invoke(true);

            Debug.Log($"[{moduleName}] Starting module");
            Debug.Log($"[{moduleName}] Module pipe: {modulePipeName}");

            // «апускаем сервер дл€ приема сообщений от оркестратора
            _ = Task.Run(async () => await StartServerAsync(_cancellationTokenSource.Token));
        }

        public void StopModule()
        {
            _isRunning = false;
            _cancellationTokenSource?.Cancel();
            _serverPipe?.Close();
            _serverPipe?.Dispose();
            OnModuleStatusChanged?.Invoke(false);
            Debug.Log($"[{moduleName}] Module stopped");
        }

        /// <summary>
        /// «апуск сервера дл€ приема сообщений от оркестратора
        /// </summary>
        private async Task StartServerAsync(CancellationToken cancellationToken)
        {
            while (_isRunning && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    _serverPipe = new NamedPipeServerStream(
                        modulePipeName,
                        PipeDirection.InOut,
                        1,
                        PipeTransmissionMode.Byte,
                        PipeOptions.Asynchronous);

                    Debug.Log($"[{moduleName}] Waiting for orchestrator connection on '{modulePipeName}'...");
                    await _serverPipe.WaitForConnectionAsync(cancellationToken);
                    Debug.Log($"[{moduleName}] Orchestrator connected!");

                    await HandleOrchestratorConnection(_serverPipe, cancellationToken);

                    _serverPipe.Disconnect();
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Debug.LogError($"[{moduleName}] Server error: {ex.Message}");
                    await Task.Delay(5000, cancellationToken);
                }
                finally
                {
                    _serverPipe?.Dispose();
                }
            }
        }

        private async Task HandleOrchestratorConnection(NamedPipeServerStream pipe, CancellationToken cancellationToken)
        {
            try
            {
                var lengthBytes = new byte[4];
                int bytesRead = await pipe.ReadAsync(lengthBytes, 0, 4, cancellationToken);

                if (bytesRead != 4) return;

                int dataLength = BitConverter.ToInt32(lengthBytes, 0);
                if (dataLength <= 0 || dataLength > 10 * 1024 * 1024)
                {
                    Debug.LogWarning($"[{moduleName}] Invalid message length: {dataLength}");
                    return;
                }

                var data = new byte[dataLength];
                bytesRead = 0;

                while (bytesRead < dataLength)
                {
                    int chunkSize = await pipe.ReadAsync(data, bytesRead, dataLength - bytesRead, cancellationToken);
                    if (chunkSize == 0) break;
                    bytesRead += chunkSize;
                }

                if (bytesRead == dataLength)
                {
                    Debug.Log($"[{moduleName}] Received raw data: {Encoding.UTF8.GetString(data)}");

                    var workItem = WorkItem.FromBytes(data);
                    if (workItem != null && workItem.IsValid())
                    {
                        Debug.Log($"[{moduleName}] Received: {workItem.type}");
                        MainThreadDispatcher.Enqueue(() => ProcessWorkItem(workItem, pipe));
                    }
                    else
                    {
                        Debug.LogWarning($"[{moduleName}] Invalid work item received");
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.LogError($"[{moduleName}] Connection handling error: {ex}");
            }
        }

        /// <summary>
        /// ќтправка ответа обратно
        /// </summary>
        private async Task SendResponseAsync(NamedPipeServerStream pipe, WorkResult result)
        {
            try
            {
                if (pipe.IsConnected)
                {
                    var resultData = result.ToBytes();
                    var lengthBytes = BitConverter.GetBytes(resultData.Length);

                    await pipe.WriteAsync(lengthBytes, 0, 4);
                    await pipe.WriteAsync(resultData, 0, resultData.Length);
                    await pipe.FlushAsync();

                    Debug.Log($"[{moduleName}] Response sent: {result.requestId}");
                }
            }
            catch (Exception ex)
            {
                Debug.LogError($"[{moduleName}] Response error: {ex.Message}");
            }
        }

        private async void ProcessWorkItem(WorkItem workItem, NamedPipeServerStream pipe)
        {
            WorkResult result = null;

            try
            {
                OnWorkItemReceived?.Invoke(workItem);
                result = await ProcessWorkItemByType(workItem);
            }
            catch (Exception ex)
            {
                result = new WorkResult
                {
                    requestId = workItem.metadata.TryGetValue("request_id", out var id) ? id : Guid.NewGuid().ToString(),
                    success = false,
                    errorMessage = ex.Message
                };
            }

            if (result != null && pipe.IsConnected)
            {
                await SendResponseAsync(pipe, result);
            }
        }

        private Task<WorkResult> ProcessWorkItemByType(WorkItem workItem)
        {
            var requestId = workItem.metadata.TryGetValue("request_id", out var id) ? id : Guid.NewGuid().ToString();

            switch (workItem.type.ToLower())
            {
                case "healthcheck":
                    Debug.Log($"[{moduleName}] Health check received: {workItem.GetDataAsString()}");
                    return Task.FromResult<WorkResult>(new WorkResult
                    {
                        requestId = requestId,
                        success = true
                    }.WithStringData("Unity module healthy"));

                case "get_game_state":
                    return HandleGetGameState(workItem, requestId);

                case "set_transform":
                    return HandleSetTransform(workItem, requestId);

                case "execute_command":
                    return HandleExecuteCommand(workItem, requestId);

                case "send_chat_to_unity":
                    return HandleChatMessage(workItem, requestId);

                default:
                    return Task.FromResult<WorkResult>(new WorkResult
                    {
                        requestId = requestId,
                        success = false,
                        errorMessage = $"Unknown type: {workItem.type}"
                    });
            }
        }

        private Task<WorkResult> HandleGetGameState(WorkItem workItem, string requestId)
        {
            var gameState = new
            {
                scene = UnityEngine.SceneManagement.SceneManager.GetActiveScene().name,
                time = Time.time,
                fps = 1.0f / Time.deltaTime,
                screenWidth = Screen.width,
                screenHeight = Screen.height,
                platform = Application.platform.ToString()
            };

            return Task.FromResult<WorkResult>(new WorkResult
            {
                requestId = requestId,
                success = true
            }.WithJsonData(gameState));
        }

        private Task<WorkResult> HandleSetTransform(WorkItem workItem, string requestId)
        {
            try
            {
                var transformData = workItem.GetDataAsJson<TransformData>();
                var obj = GameObject.Find(transformData.ObjectName);

                if (obj != null)
                {
                    obj.transform.position = new Vector3(transformData.X, transformData.Y, transformData.Z);
                    return Task.FromResult<WorkResult>(new WorkResult
                    {
                        requestId = requestId,
                        success = true
                    });
                }
                return Task.FromResult<WorkResult>(new WorkResult
                {
                    requestId = requestId,
                    success = false,
                    errorMessage = $"Object '{transformData.ObjectName}' not found"
                });
            }
            catch (Exception ex)
            {
                return Task.FromResult<WorkResult>(new WorkResult
                {
                    requestId = requestId,
                    success = false,
                    errorMessage = ex.Message
                });
            }
        }

        private Task<WorkResult> HandleExecuteCommand(WorkItem workItem, string requestId)
        {
            var command = workItem.GetDataAsString();
            Debug.Log($"[{moduleName}] Executing: {command}");

            return Task.FromResult<WorkResult>(new WorkResult
            {
                requestId = requestId,
                success = true
            }.WithStringData($"Executed: {command}"));
        }

        private Task<WorkResult> HandleChatMessage(WorkItem workItem, string requestId)
        {
            var message = workItem.GetDataAsString();
            Debug.Log($"[{moduleName}] Chat: {message}");

            return Task.FromResult<WorkResult>(new WorkResult
            {
                requestId = requestId,
                success = true
            }.WithStringData("Message processed"));
        }

        private void OnDestroy()
        {
            StopModule();
            _cancellationTokenSource?.Dispose();
        }

        [System.Serializable]
        public class TransformData
        {
            public string ObjectName = string.Empty;
            public float X = 0f;
            public float Y = 0f;
            public float Z = 0f;
        }
    }

    public static class MainThreadDispatcher
    {
        private static readonly Queue<Action> _executionQueue = new Queue<Action>();

        public static void Enqueue(Action action)
        {
            lock (_executionQueue) _executionQueue.Enqueue(action);
        }

        public static void ExecutePending()
        {
            lock (_executionQueue)
                while (_executionQueue.Count > 0)
                    _executionQueue.Dequeue()?.Invoke();
        }
    }
}