using Microsoft.AspNetCore.Mvc;

namespace TouchdownAndBigPlayAlertApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class TouchdownAndBigPlayAlertController : ControllerBase
    {
        private readonly ParsePlayByPlay _parsePlayByPlay;

        public TouchdownAndBigPlayAlertController(ParsePlayByPlay parsePlayByPlay)
        {
            _parsePlayByPlay = parsePlayByPlay;
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
                await _parsePlayByPlay.RunParser();
                return Ok(new { message = "Games processed successfully" });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { error = ex.Message });
            }
        }
    }
}