using Microsoft.AspNetCore.Mvc;

namespace TouchdownAndBigPlayAlertApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class TouchdownAndBigPlayAlertController : ControllerBase
    {
        private readonly ParsePlayByPlay _parsePlayByPlay;

        /// <summary>
        /// 
        /// </summary>
        //private readonly IOptions<AppConfiguration> _config;
        private readonly IConfiguration _config;

        public TouchdownAndBigPlayAlertController(ParsePlayByPlay parsePlayByPlay, IConfiguration config)
        {
            _parsePlayByPlay = parsePlayByPlay;
            _config = config;
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
                await _parsePlayByPlay.RunParser(_config);
                return Ok(new { message = "Games processed successfully" });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { error = ex.Message });
            }
        }
    }
}