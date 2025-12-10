using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Mvc;

namespace TouchdownAndBigPlayAlertApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class TouchdownAndBigPlayAlertController : ControllerBase
    {
        private readonly ParsePlayByPlay _parsePlayByPlay;
        private readonly IConfiguration _config;
        private readonly ILogger<TouchdownAndBigPlayAlertController> _logger;

        public TouchdownAndBigPlayAlertController(
            ParsePlayByPlay parsePlayByPlay, 
            IConfiguration config,
            ILogger<TouchdownAndBigPlayAlertController> logger)
        {
            _parsePlayByPlay = parsePlayByPlay;
            _config = config;
            _logger = logger;
        }

        /// <summary>
        /// Simple health check endpoint
        /// </summary>
        /// <returns></returns>
        [HttpGet("health")]
        public IActionResult Health()
        {
            return Ok(new { status = "API is running", timestamp = DateTime.UtcNow });
        }

        /// <summary>
        /// Simple POST test endpoint
        /// </summary>
        /// <returns></returns>
        [HttpPost("test")]
        public IActionResult TestPost()
        {
            return Ok(new { message = "POST endpoint works", timestamp = DateTime.UtcNow });
        }

        [HttpGet("check-service-bus-messages")]
        public async Task<IActionResult> CheckServiceBusMessages()
        {
            try
            {
                string serviceBusNamespace = "fantasyfootballstattracker.servicebus.windows.net";
                string queueName = "touchdownqueue";

                ServiceBusClient client = new ServiceBusClient(serviceBusNamespace, new DefaultAzureCredential());
                ServiceBusReceiver receiver = client.CreateReceiver(queueName, new ServiceBusReceiverOptions
                {
                    ReceiveMode = ServiceBusReceiveMode.PeekLock
                });

                var messages = await receiver.PeekMessagesAsync(maxMessages: 10);

                var messageDetails = messages.Select(msg => new
                {
                    MessageId = msg.MessageId,
                    ContentType = msg.ContentType,
                    Body = msg.Body.ToString(),
                    Properties = msg.ApplicationProperties,
                    EnqueuedTime = msg.EnqueuedTime,
                    Size = msg.Body.ToArray().Length
                }).ToList();

                await receiver.DisposeAsync();
                await client.DisposeAsync();

                return Ok(new
                {
                    MessageCount = messages.Count,
                    Messages = messageDetails
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { error = ex.Message, details = ex.ToString() });
            }
        }

        /// <summary>
        /// Parses touchdowns and big plays for each game that each player in the active rosters for both owners
        /// are playing in.
        /// </summary>
        /// <returns></returns>
        [HttpPost("parse-games")]
        public async Task<IActionResult> ParseGames()
        {
            try
            {
                _logger.LogInformation("Starting parse-games endpoint at {Timestamp}", DateTime.UtcNow);
                
                await _parsePlayByPlay.RunParser(_config);
                
                _logger.LogInformation("Successfully completed parse-games endpoint at {Timestamp}", DateTime.UtcNow);
                return Ok(new { message = "Games processed successfully", timestamp = DateTime.UtcNow });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "CRITICAL ERROR in parse-games endpoint: {ErrorMessage}. StackTrace: {StackTrace}", 
                    ex.Message, ex.StackTrace);
                
                // Log inner exception if it exists
                if (ex.InnerException != null)
                {
                    _logger.LogError("Inner Exception: {InnerMessage}. InnerStackTrace: {InnerStackTrace}", 
                        ex.InnerException.Message, ex.InnerException.StackTrace);
                }
                
                return StatusCode(500, new { 
                    error = ex.Message,
                    type = ex.GetType().Name,
                    innerError = ex.InnerException?.Message,
                    stackTrace = ex.StackTrace
                });
            }
        }
    }
}