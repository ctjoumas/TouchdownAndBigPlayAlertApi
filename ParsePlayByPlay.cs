namespace TouchdownAndBigPlayAlertApi
{
    using Azure.Core;
    using Azure.Identity;
    using Azure.Messaging.ServiceBus;
    using HtmlAgilityPack;
    using Microsoft.Data.SqlClient;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System.Collections;

    public class ParsePlayByPlay
    {
        private readonly ILogger<ParsePlayByPlay> _logger;

        /// <summary>
        /// Session key for the Azure SQL Access token
        /// </summary>
        public const string SessionKeyAzureSqlAccessToken = "_Token";

        /// <summary>
        /// Root of the play by play URL where we will get the play by play json object
        /// </summary>
        private const string PLAY_BY_PLAY_URL = "https://www.espn.com/nfl/playbyplay/_/gameId/";

        private const int RECEIVING_AND_RUSHING_BIG_PLAY_YARDAGE = 25;
        private const int PASSING_BIG_PLAY_YARDAGE = 40;

        public ParsePlayByPlay(ILogger<ParsePlayByPlay> logger)
        {
            _logger = logger;
        }

        public async Task RunParser(IConfiguration config)
        {
            Hashtable gamesToParse = getGamesToParse(_logger);

            await parseTouchdownsAndBigPlays(gamesToParse, config, _logger);
        }

        /// <summary>
        /// Gets the rosters for the latest/current week from the CurrentRoster table and stores it
        /// in a hashtable where the key is the ESPN Game ID and the value are all players playing
        /// in that game.
        /// </summary>
        /// <param name="log">Logger</param>
        /// <returns>Hashtable of games for each player from the current weeks roster</returns>
        private Hashtable getGamesToParse(ILogger log)
        {
            Hashtable gamesToParse = new Hashtable();

            var connectionStringBuilder = new SqlConnectionStringBuilder
            {
                DataSource = "tcp:playersandschedulesdetails.database.windows.net,1433",
                InitialCatalog = "PlayersAndSchedulesDetails",
                TrustServerCertificate = false,
                Encrypt = true
            };

            SqlConnection sqlConnection = new SqlConnection(connectionStringBuilder.ConnectionString);

            try
            {
                string azureSqlToken = GetAzureSqlAccessToken();
                sqlConnection.AccessToken = azureSqlToken;
            }
            catch (Exception e)
            {
                log.LogInformation(e.Message);
            }

            using (sqlConnection)
            {
                sqlConnection.Open();

                // call stored procedure to get all players for each team's roster for this week
                using (SqlCommand command = new SqlCommand("GetTeamsForCurrentWeek", sqlConnection))
                {
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int season = (int)reader.GetValue(reader.GetOrdinal("Season"));
                            int ownerId = (int)reader.GetValue(reader.GetOrdinal("OwnerID"));
                            string ownerName = reader.GetValue(reader.GetOrdinal("OwnerName")).ToString();
                            string ownerPhoneNumber = reader.GetValue(reader.GetOrdinal("PhoneNumber")).ToString();
                            string playerName = reader.GetValue(reader.GetOrdinal("PlayerName")).ToString();
                            string teamAbbreviation = reader.GetValue(reader.GetOrdinal("TeamAbbreviation")).ToString();
                            string opponentAbbreviation = reader.GetValue(reader.GetOrdinal("OpponentAbbreviation")).ToString();
                            bool gameEnded = (bool)reader.GetValue(reader.GetOrdinal("GameEnded"));
                            DateTime gameDate = DateTime.Parse((reader.GetValue(reader.GetOrdinal("GameDate")).ToString()));
                            string espnGameId = reader.GetValue(reader.GetOrdinal("EspnGameId")).ToString();

                            // Get current EST time - If this is run on a machine with a differnet local time, DateTime.Now will not return the proper time
                            TimeZoneInfo easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
                            DateTime currentEasterStandardTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, easternZone);
                            TimeSpan difference = gameDate.Subtract(currentEasterStandardTime);

                            // if the game hasn't started or the game has ended, don't load the HtmlDoc to parse stats since we've already done that
                            if ((difference.TotalDays < 0) && (!gameEnded))
                            {
                                PlayDetails playDetails = new PlayDetails();
                                playDetails.Season = season;
                                playDetails.OwnerId = ownerId;
                                playDetails.OwnerName = ownerName;
                                playDetails.PhoneNumber = ownerPhoneNumber;
                                playDetails.TeamAbbreviation = teamAbbreviation;
                                playDetails.OpponentAbbreviation = opponentAbbreviation;
                                playDetails.GameDate = gameDate;
                                playDetails.PlayerName = playerName;

                                // it's more expensive to use the ContainsKey method on a hashtable, so just pull out
                                // the value and check if it's null
                                List<PlayDetails> playerList = (List<PlayDetails>)gamesToParse[espnGameId];

                                // if it's not null, the game exists in the hashtable, so let's remove the item so we can add the
                                // touchdown details for this player to the list and re-add the key/value pair with this new player's
                                // touchdown details. Oherwise, we will create an empty ArrayList for the players touchdown details so
                                // we can add the touchdown details and put the new game key/value pair into the hashtable
                                if (playerList != null)
                                {
                                    gamesToParse.Remove(espnGameId);
                                }
                                else
                                {
                                    playerList = new List<PlayDetails>();
                                }

                                playerList.Add(playDetails);
                                gamesToParse.Add(espnGameId, playerList);

                                log.LogInformation("player name: " + playerName + "(" + ownerPhoneNumber + ")");
                            }
                        }
                    }
                }

                sqlConnection.Close();
            }

            return gamesToParse;
        }

        /// <summary>
        /// Parses touchdowns for each game that each player in the active rosters for both owners are playing in.
        /// </summary>
        /// <param name="gamesToParse">Key is the Espn Game ID and the value is the list of players playing in the game</param>
        /// <param name="log">Logger</param>
        public async Task parseTouchdownsAndBigPlays(Hashtable gamesToParse, IConfiguration configuration, ILogger log)
        {
            JObject playByPlayJsonObject;

            // go through each key (game id) in the hashtable and parse these games' JSON play
            // by play checking for each player (value, which is a list of players) playing in
            // that game
            foreach (var key in gamesToParse.Keys)
            {
                log.LogInformation("Key for play by play URL is: " + key);

                string playByPlayUrl = PLAY_BY_PLAY_URL + key;
                HtmlDocument playByPlayDoc = new HtmlWeb().Load(playByPlayUrl);

                // get the play by play JSON object for this game
                playByPlayJsonObject = GetPlayByPlayJsonObject(playByPlayDoc, log);

                if (playByPlayJsonObject == null)
                {
                    log.LogInformation("Play by play JSON object is NULL! at " + DateTime.Now);
                }
                else
                {
                    log.LogInformation("Play by play JSON is good at " + DateTime.Now);

                    List<PlayDetails> playersInGame = (List<PlayDetails>)gamesToParse[key];

                    await ParsePlayerBigPlaysForGame(int.Parse((string)key), playByPlayJsonObject, playersInGame, configuration, log);
                    await ParsePlayerTouchdownsForGame(int.Parse((string)key), playByPlayJsonObject, playersInGame, configuration, log);
                }
            }
        }

        /// <summary>
        /// When a game is in progress, the play by play data is updated in the javascript function's espn.gamepackage.data variable.
        /// This method will find this variable and pull out the JSON and store it so we can parse the live play by play data.
        /// </summary>
        /// <param name="playByPlayDoc">The play by play document for a particular game</param>
        /// <returns>The JSON object representing the play by play data.</returns>
        public JObject GetPlayByPlayJsonObject(HtmlDocument playByPlayDoc, ILogger log)
        {
            JObject playByPlayJsonObject = null;

            var playByPlayJavaScriptNodes = playByPlayDoc.DocumentNode.SelectNodes("//script");

            foreach (var scriptNode in playByPlayJavaScriptNodes)
            {
                // the script will have:
                // window['__espnfitt__'] = { "app": {.... <all json> }
                if (scriptNode.InnerText.Contains("window['__espnfitt__']"))
                {
                    // as of August 2023, the first part of the JSON will have a node before the window['__espnfitt__']=,
                    // so we need to start the search for the JSON where this element starts
                    string content = scriptNode.InnerText.Trim();
                    int equalIndex = content.IndexOf("=", content.IndexOf("window['__espnfitt__']"));

                    // there is a trailing ;, so pull that off
                    string jsonContent = content.Substring(equalIndex + 1, content.Length - (equalIndex + 2));

                    playByPlayJsonObject = JObject.Parse(jsonContent);

                    break;
                }
            }

            return playByPlayJsonObject;
        }

        private async Task ParsePlayerBigPlaysForGame(int espnGameId, JObject playByPlayJsonObject, List<PlayDetails> playersInGame, IConfiguration configuration, ILogger log)
        {
            JToken driveTokens = playByPlayJsonObject.SelectToken("page.content.gamepackage.allPlys");

            // TODO: This logic will go through each drive. However, during a live game, we should only pull the last play of the drive, assuming that
            // the other drives were already parsed based on how frequently the logic app calls this endpoint.
            foreach (JToken quarterToken in driveTokens)
            {
                JToken quarterDrives = quarterToken.SelectToken("items");

                foreach (JToken quarterDrive in quarterDrives)
                {
                    // get all of the plays in this drive
                    JToken playTokens = quarterDrive.SelectToken("plays");

                    if (playTokens != null)
                    {
                        foreach (var playToken in playTokens)
                        {
                            string playResult = playToken.SelectToken("description").ToString();

                            // for any play that results in yardage, it will have the word "yards", such as:
                            // For Rushing: we only need to check for the word "yards", since "yard" will just be for 1 yard and then verify it's a rush (absence of word "pass")
                            //   "(9:12 - 1st) J.Conner up the middle to ARZ 27 for 23 yards (Ma.Jones)."
                            //   "(10:01 - 1st) R.Stevenson up the middle to ARZ 45 for 1 yard (M.Sanders)." - IGNORE THIS
                            //   "(13:51 - 1st) (No Huddle, Shotgun) K.Murray scrambles right end to ARZ 44 for 3 yards (M.Judon). ARZ-K.Murray was injured during the play." - scramble for QB
                            //
                            // For Rushing but a fumble included: we will only care about the runner - even if they fumbled, they could have had a long run - this will be before the word "FUMBLES"
                            //   "(8:45 - 2nd) (Shotgun) J.Wilson up the middle to MIA 47 for 6 yards (A.Gilman). FUMBLES (A.Gilman), touched at MIA 44, recovered by MIA-T.Hill at MIA 43. T.Hill for 57 yards, TOUCHDOWN.J.Sanders extra point is GOOD, Center-B.Ferguson, Holder-T.Morstead."
                            //
                            // For Passing / Receiving: we need to check for passer and receiver since receiver will get a big play with less yardage than a passer
                            //   "(10:21 - 4th) (Shotgun) K.Cousins pass short middle to D.Cook to MIN 26 for 13 yards (J.Blackmon; B.Okereke)."
                            //   "(1:24 - 2nd) (Shotgun) J.Herbert pass deep right to J.Palmer ran ob at MIA 9 for 18 yards."
                            //
                            // Plays we want to ignore:
                            //   Incomplete Passes: these do not have the word "yards"
                            //     "(11:18 - 1st) (Shotgun) C.McCoy pass incomplete short left to D.Hopkins (Ja.Jones)."
                            //   Field Goals: these have the word "yard" and not "yards"
                            //     "(10:39 - 1st) M.Prater 50 yard field goal is No Good, Wide Left, Center-A.Brewer, Holder-A.Lee."
                            //   Punts: these have the word "yards" but has the word "punts", so we'll check for that
                            //     "(9:18 - 1st) M.Palardy punts 42 yards to ARZ 8, Center-J.Cardona, fair catch by G.Dortch. PENALTY on ARZ-C.Matthew, Offensive Holding, 4 yards, enforced at ARZ 8."
                            //   Kickoffs: these have the word "yards" but has the word "kicks", so we'll check for that
                            //   Penalties: these have the word "yards", but also the word "PENALTY" and "No Play", so we can check for both to be safe
                            //     "(9:22 - 1st) (Shotgun) PENALTY on NE-T.Brown, False Start, 5 yards, enforced at ARZ 45 - No Play."
                            //   Sacks: these have the word "yards", but yardage will be negative, but we will check for the word "sacked" so we don't parse the yardage
                            //     "(7:21 - 1st) (No Huddle, Shotgun) C.McCoy sacked at ARZ 38 for -5 yards (sack split by M.Judon and L.Guy)."
                            //   Interceptions: these have the word "yards" but also the word "INTERCEPTED", so we'll ignore that
                            //     "(2:15 - 1st) M.Jones pass short middle intended for T.Thornton INTERCEPTED by I.Simmons (C.Thomas) at NE 41. I.Simmons to NE 36 for 5 yards (Ma.Jones)."
                            if (playResult.ToLower().Contains("yards") && !playResult.ToLower().Contains("punts") &&
                                !playResult.ToLower().Contains("penalty") && !playResult.ToLower().Contains("intercepted") &&
                                !playResult.ToLower().Contains("kicks") && !playResult.ToLower().Contains("touchdown"))
                            {
                                // used to determine if a big play occured, whether it's passing, receiving, or rushing
                                bool bigPlayOccurred = false;

                                // if a fumble occurs, we need to cut off the text from FUMBLES on so that the recovering player doesn't get credited with the big play.
                                // We save this original play so during the processing of the big play, we can check to see if the other team recovered the fumble so we
                                // can add that to the alert
                                string originalPlayResult = playResult;

                                // if it's a fumble recovery cut off the string from the word FUMBLES on since forward progress on a fumble recovery is not credited to a player
                                if (playResult.ToLower().Contains("fumbles"))
                                {
                                    playResult = playResult.Substring(0, playResult.ToLower().IndexOf("fumbles"));
                                }

                                // pull out the yardage to see if this is a big play before we loop through all of the players
                                int playYards = GetPlayYardage(playResult);

                                // a passing or receiving play requires less yardage than a pass play to be considered a big play,
                                // so that is our minimum threshold for a big play; if we don't have that, we can skip this play
                                // we can get the yardage of the play from the statYardage property
                                if (playYards >= RECEIVING_AND_RUSHING_BIG_PLAY_YARDAGE)
                                {
                                    // get the details of the touchdown
                                    // we will cache the quarter and game clock so the next time we check the live JSON data, we don't
                                    // send a message to the service bus that the same touchdown was scored
                                    // We'll parse it based on the play text starting with either:
                                    // 1st quarter: "(9:59 - 1st) 
                                    // 2nd quarter: "(11:50 - 2nd)
                                    // 3rd quarter: "(14:12 - 3rd)
                                    // 4th quarter: "(10:42 - 4th)
                                    // OT: "(10:00 - OT) (we'll use the number 5 for OT)
                                    string gameClock = playResult.Substring(1, playResult.IndexOf(" ") - 1);
                                    int quarter = GetQuarter(playResult);

                                    foreach (PlayDetails playDetails in playersInGame)
                                    {
                                        // check if the player is involved in this play
                                        // get the player name as first <initial>.<lastname> to check if this is the player
                                        // who scored a touchdown
                                        string abbreviatedPlayerName = playDetails.PlayerName;
                                        int spaceIndex = abbreviatedPlayerName.IndexOf(' ');
                                        abbreviatedPlayerName = abbreviatedPlayerName[0] + "." + abbreviatedPlayerName.Substring(spaceIndex + 1);

                                        // if this player was involved in the play, let's determine the type of play
                                        if (playResult.Contains(abbreviatedPlayerName))
                                        {
                                            // If this is a pass play, we need to determine if this player threw the ball or received it
                                            //   "(10:21 - 4th) (Shotgun) K.Cousins pass short middle to D.Cook to MIN 26 for 13 yards (J.Blackmon; B.Okereke)."
                                            //   "(1:24 - 2nd) (Shotgun) J.Herbert pass deep right to J.Palmer ran ob at MIA 9 for 18 yards."
                                            if (playResult.ToLower().Contains("pass"))
                                            {
                                                // If the occurence of the word "pass" occurs after the player name, then this player threw the pass;
                                                // otherwise, the player received it
                                                if (playResult.IndexOf(abbreviatedPlayerName) < playResult.IndexOf("pass"))
                                                {
                                                    // get the name of the receiver who caught the pass
                                                    string receiversName = GetReceivingPlayerNameInPlay(playResult);

                                                    // player threw a pass, so we'll only alert if it's above the passing yardage threshold
                                                    if (playYards >= PASSING_BIG_PLAY_YARDAGE)
                                                    {
                                                        bigPlayOccurred = true;

                                                        log.LogInformation("*** " + "🚀 Big play! " + playDetails.PlayerName + " threw a pass of " + playYards + " yards to " + receiversName + "!\n\n");

                                                        playDetails.Message = "🚀 Big play! " + playDetails.PlayerName + " threw a pass of " + playYards + " yards to " + receiversName + "!";
                                                    }
                                                }
                                                else
                                                {
                                                    bigPlayOccurred = true;

                                                    log.LogInformation("*** " + "🚀 Big play! " + playDetails.PlayerName + " caught a pass of " + playYards + " yards.\n\n");

                                                    // player received a pass, and we already know it's above the threshold since that was our
                                                    // first check, so just send the alert
                                                    playDetails.Message = "🚀 Big play! " + playDetails.PlayerName + " caught a pass of " + playYards + " yards.";

                                                    // if the player fumbled, let's see if the other team recovered it so we can add that to the message
                                                    if (originalPlayResult.ToLower().Contains("fumbles"))
                                                    {
                                                        // get recovering team name so if it's the opponent, we can say while this was a big play, it was also a lost fumble
                                                        int indexOfRecoveredBy = originalPlayResult.IndexOf("recovered by");
                                                        int indexOfDash = originalPlayResult.IndexOf("-", indexOfRecoveredBy);
                                                        string recoveringTeamAbbreviation = originalPlayResult.Substring(indexOfRecoveredBy + "recovered by".Length + 1, indexOfDash - (indexOfRecoveredBy + "recovered by".Length + 1));

                                                        if (recoveringTeamAbbreviation.ToLower().Equals(playDetails.OpponentAbbreviation.ToLower()))
                                                        {
                                                            log.LogInformation("(FUMBLE - Lost ball on the play)");
                                                            playDetails.Message += " (FUMBLE - Lost ball on the play)";
                                                        }
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                bigPlayOccurred = true;

                                                log.LogInformation("*** " + "🚀 Big play! " + playDetails.PlayerName + " rushed for " + playYards + " yards.\n\n");

                                                playDetails.Message = "🚀 Big play! " + playDetails.PlayerName + " rushed for " + playYards + " yards.";

                                                // if the player fumbled, let's see if the other team recovered it so we can add that to the message
                                                if (originalPlayResult.ToLower().Contains("fumbles"))
                                                {
                                                    // get recovering team name so if it's the opponent, we can say while this was a big play, it was also a lost fumble
                                                    int indexOfRecoveredBy = originalPlayResult.IndexOf("recovered by");
                                                    int indexOfDash = originalPlayResult.IndexOf("-", indexOfRecoveredBy);
                                                    string recoveringTeamAbbreviation = originalPlayResult.Substring(indexOfRecoveredBy + "recovered by".Length + 1, indexOfDash - (indexOfRecoveredBy + "recovered by".Length + 1));

                                                    if (recoveringTeamAbbreviation.ToLower().Equals(playDetails.OpponentAbbreviation.ToLower()))
                                                    {
                                                        log.LogInformation("(FUMBLE - Lost ball on the play)");
                                                        playDetails.Message += " (FUMBLE - Lost ball on the play)";
                                                    }
                                                }
                                            }

                                            // if a big play occurred, let's add it to the database
                                            if (bigPlayOccurred)
                                            {
                                                // if this big play by this player was not already parsed, the big play will be added
                                                bool bigPlayAdded = AddBigPlayDetails(espnGameId, quarter, gameClock, playDetails.PlayerName, playDetails.Season, playDetails.OwnerId, playDetails.OpponentAbbreviation, playDetails.GameDate, log);

                                                if (bigPlayAdded)
                                                {
                                                    log.LogInformation("Added big play for " + playDetails.PlayerName);
                                                    await sendPlayMessage(playDetails);
                                                }
                                                else
                                                {
                                                    log.LogInformation("Did NOT log big play for " + playDetails.PlayerName + "; big play already parsed earlier.");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// With the new update to the JSON doc, parsing touchdowns in a drive is not as easy as each play no longer has the
        /// necessary details and the text description is fairly complex. There is now a scrPlayGrps node which has only
        /// scoring plays, which we can check for TDs.
        /// </summary>
        /// <param name="espnGameId"></param>
        /// <param name="playByPlayJsonObject"></param>
        /// <param name="playersInGame"></param>
        private async Task ParsePlayerTouchdownsForGame(int espnGameId, JObject playByPlayJsonObject, List<PlayDetails> playersInGame, IConfiguration configuration, ILogger log)
        {
            // flag determining whether or not a touchdown was processed for a player so we know if we should add this
            // to the database and send the message to the service hub so the logic app will process it and send a text
            // message to the owner
            bool touchdownProcessed = false;

            // As of 2025, this is now broken up by quarter, so we'll get one token for all quarters scoring summaries (plays)
            JToken allQuartersScoringSummaries = (JArray)playByPlayJsonObject.SelectToken("page.content.gamepackage.scrSumm");

            // go through each quarter's scoring summaries
            foreach (JToken quarterScoringSummaries in allQuartersScoringSummaries)
            {
                // get all scoring plays for this quarter
                JToken quarterScoringPlays = quarterScoringSummaries.SelectToken("items");

                foreach (JToken quarterScoringPlay in quarterScoringPlays)
                {
                    // the typeAbbreviation attribute will be "TD" for a touchdown
                    string scoringType = ((JValue)quarterScoringPlay.SelectToken("typeAbbreviation")).Value.ToString();

                    if (scoringType.Equals("TD"))
                    {
                        string touchdownText = ((JValue)quarterScoringPlay.SelectToken("playText")).Value.ToString();

                        // we will cache the quarter and game clock so the next time we check the live JSON data, we don't
                        // send a message to the service bus that the same touchdown was scored
                        int quarter = int.Parse(quarterScoringPlay.SelectToken("periodNum").ToString());
                        string gameClock = (string)((JValue)quarterScoringPlay.SelectToken("clock")).Value;

                        // if this is a defensive touchdown, the defense name is not listed in the text, so if the text falls
                        // into this case, we won't loop through all players, but we'll find out based on the teamId of this
                        // scoring play which defense scored and check to see if an owner has this defense. Otherwise, this is
                        // an offensive TD and we'll go to the else if condition and check all players on the owner's roster
                        // eccept for the defenses. The cases we know of so far are:
                        // 1. Blocked punt returned for a TD (text actually shows Blocked Kick)
                        //   - "Blocked Kick Recovered by JoJo Domann (IND), C.McLaughlin extra point is GOOD, Center-L.Rhodes, Holder-M.Haack."
                        // 2. Pick six (text shows Interception Return)
                        //   - "Julian Blackmon 17 Yd Interception Return, C.McLaughlin extra point is GOOD, Center-L.Rhodes, Holder-M.Haack."
                        // 3. Punt return for a TD (text shows Punt Return)
                        //   - "Calvin Austin III 73 Yd Pun Return (Chris Boswell Kick)"
                        // 4. Fumble recovery for TD?
                        // 5. Kick return?
                        if (touchdownText.ToLower().Contains("blocked kick"))
                        {
                            PlayDetails playDetails = GetDefenseWhoScoredTouchdown(playByPlayJsonObject, quarterScoringPlay, playersInGame);

                            string touchdownMessage = "🎉 Defensive Touchdown! " + playDetails.PlayerName + " blocked a kick and returned it for a TD!";

                            await SendDefensiveTouchdownMessage(espnGameId, playDetails, quarterScoringPlay, playersInGame, quarter, gameClock, "", configuration, log);
                        }
                        else if (touchdownText.ToLower().Contains("interception return"))
                        {
                            PlayDetails playDetails = GetDefenseWhoScoredTouchdown(playByPlayJsonObject, quarterScoringPlay, playersInGame);

                            string touchdownMessage = "🎉 Defensive Touchdown! " + playDetails.PlayerName + " just got a pick 6!";

                            await SendDefensiveTouchdownMessage(espnGameId, playDetails, quarterScoringPlay, playersInGame, quarter, gameClock, "", configuration, log);
                        }
                        else if (touchdownText.ToLower().Contains("punt return"))
                        {
                            PlayDetails playDetails = GetDefenseWhoScoredTouchdown(playByPlayJsonObject, quarterScoringPlay, playersInGame);

                            string touchdownMessage = "🎉 Defensive Touchdown! " + playDetails.PlayerName + " just returned a punt for a TD!";

                            await SendDefensiveTouchdownMessage(espnGameId, playDetails, quarterScoringPlay, playersInGame, quarter, gameClock, "", configuration, log);
                        }
                        // it's an offensive TD
                        else
                        {
                            // check if any of players in the players list (current roster) have scored
                            foreach (PlayDetails playDetails in playersInGame)
                            {
                                // It appears that the a player who rushed or received a TD will have their name appear as the first part of the text
                                // and the QB will appear after the "from" text such as:
                                // Rush:
                                //   "Christian McCaffrey 1 Yd Rush, R.Gould extra point is GOOD, Center-T.Pepper, Holder-M.Wishnowsky."
                                //   "Austin Ekeler 1 Yd Run (Cameron Dicker Kick)" or
                                //   
                                // Pass (it looks like the first one here is what is shown during live games; when games end, it changes to the 2nd, so we should only really
                                // care about the first one)
                                //   "George Kittle Pass From Brock Purdy for 28 Yds, R.Gould extra point is GOOD, Center-T.Pepper, Holder-M.Wishnowsky."
                                //   "Tyreek Hill 60 Yd pass from Tua Tagovailoa (Jason Sanders Kick)" (this will work for both WR/RB and QB) or
                                //   
                                // Fumble Recovery:
                                //   "Tyreek Hill 57 Yd Fumble Recovery (Jason Sanders Kick)" for an offensive fumble recovery for a TD
                                // let's check for a player who rushed or received a TD or picked up an offensive fumble and ran it in for a TD

                                if (touchdownText.StartsWith(playDetails.PlayerName))
                                {
                                    // regardless of the play, we need to get the yardage
                                    int touchdownPlayYardage = GetTouchdownPlayYardage(touchdownText);

                                    string passingPlayer = "";

                                    // if this is a pass, the word "pass" will be in the text and we need to pull out the name of the player
                                    // who threw the TD
                                    if (touchdownText.ToLower().Contains("pass"))
                                    {
                                        touchdownProcessed = true;

                                        passingPlayer = GetPassingPlayerNameInTouchdown(touchdownText);

                                        playDetails.Message = "🎉 Touchdown! " + playDetails.PlayerName + " caught a " + touchdownPlayYardage + " yard TD from " + passingPlayer + "!";
                                    }
                                    // otherwise if it's a fumble recovery for a TD
                                    else if (touchdownText.ToLower().Contains("fumble recovery"))
                                    {
                                        touchdownProcessed = true;

                                        playDetails.Message = "🎉 Touchdown! " + playDetails.PlayerName + " recovered a fumble for a " + touchdownPlayYardage + " yard TD!";
                                    }
                                    // otherwise, i'ts a rushing TD
                                    else if (touchdownText.ToLower().Contains("run") || touchdownText.ToLower().Contains("rush"))
                                    {
                                        touchdownProcessed = true;

                                        playDetails.Message = "🎉 Touchdown! " + playDetails.PlayerName + " ran for a " + touchdownPlayYardage + " yard TD!";
                                    }
                                    else
                                    {
                                        log.LogInformation("Unknown! Play text: " + touchdownText);
                                    }
                                }
                                // otherwise, if this player name is in the text, then they threw a TD pass, such as this one from Brock Purdy
                                // "George Kittle Pass From Brock Purdy for 28 Yds, R.Gould extra point is GOOD, Center-T.Pepper, Holder-M.Wishnowsky."
                                // This next one is only in this format with the parens for the kicker after the game ends
                                // "Tyreek Hill 60 Yd pass from Tua Tagovailoa (Jason Sanders Kick)"
                                else if (touchdownText.Contains(playDetails.PlayerName) &&
                                        ((touchdownText.IndexOf(playDetails.PlayerName) < touchdownText.IndexOf("(")) || (touchdownText.IndexOf(playDetails.PlayerName) < touchdownText.IndexOf(","))))
                                {
                                    touchdownProcessed = true;

                                    string passingPlayer = GetPassingPlayerNameInTouchdown(touchdownText);

                                    // get the name of the player this player threw a TD to
                                    string[] wordsInTouchdownText = touchdownText.Split(" ");

                                    // get the integer in this string, which will be the yardage of the play
                                    int touchdownPlayYardage = GetTouchdownPlayYardage(touchdownText);

                                    // now that we have the yardage, we can grab the players name to the left of this, which is the name of the
                                    // player this QB threw a touchdown to
                                    string receivingPlayer = touchdownText.Substring(0, touchdownText.ToLower().IndexOf("pass from") - 1);

                                    // if the format is like "tyreek Hill 60 Yd pass from...", the above will have "Tyreek Hill 60 Yd" for the name, so we
                                    // need to check for this and strip it off
                                    if (receivingPlayer.ToLower().Contains("yd"))
                                    {
                                        receivingPlayer = receivingPlayer.Substring(0, receivingPlayer.IndexOf(touchdownPlayYardage.ToString()) - 1);
                                    }

                                    playDetails.Message = "🎉 Touchdown! " + playDetails.PlayerName + " threw a " + touchdownPlayYardage + " yard TD to " + receivingPlayer + "!";
                                }

                                // if a touchdown was processed, add the touchdown to the db and send the message to the service hub
                                if (touchdownProcessed)
                                {
                                    // if this touchdown scored by this player was not already parsed, the touchdown will be added
                                    bool touchdownAdded = AddTouchdownDetails(espnGameId, quarter, gameClock, playDetails.PlayerName, playDetails.Season, playDetails.OwnerId, playDetails.OpponentAbbreviation, playDetails.GameDate, log);

                                    if (touchdownAdded)
                                    {
                                        log.LogInformation(playDetails.Message);

                                        await sendPlayMessage(playDetails);
                                    }
                                    else
                                    {
                                        log.LogInformation("Did NOT log TD for " + playDetails.PlayerName + "; TD already parsed earlier.");
                                    }

                                    // reset the touchdown processed flag
                                    touchdownProcessed = false;
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Sends a text to the owner of the defense who scored a touchdown.
        /// </summary>
        /// <param name="espnGameId">Game ID of the game this defense if playing in</param>
        /// <param name="playDetails">The details of the TD play</param>
        /// <param name="scoringPlayToken">The scoring play JSON token</param>
        /// <param name="playersInGame">List of the players in the game</param>
        /// <param name="quarter">The querter the TD occurred</param>
        /// <param name="gameClock">The time the TD occurred</param>
        /// <param name="playMessage">The message to be sent</param>
        private async Task SendDefensiveTouchdownMessage(int espnGameId, PlayDetails playDetails, JToken scoringPlayToken, List<PlayDetails> playersInGame, int quarter, string gameClock, string playMessage, IConfiguration configuration, ILogger log)
        {
            // if this object isn't null, an owner has this defense, so send the text alert
            if (playDetails != null)
            {
                // if this touchdown scored by this defense was not already parsed, the touchdown will be added
                bool touchdownAdded = AddTouchdownDetails(espnGameId, quarter, gameClock, playDetails.PlayerName, playDetails.Season, playDetails.OwnerId, playDetails.OpponentAbbreviation, playDetails.GameDate, log);

                if (touchdownAdded)
                {
                    playDetails.Message = playMessage;

                    log.LogInformation(playDetails.Message);

                    await sendPlayMessage(playDetails);
                }
                else
                {
                    log.LogInformation("Did NOT log TD for " + playDetails.PlayerName + "; TD already parsed earlier.");
                }
            }
        }

        /// <summary>
        /// Given a touchdown string from the touchdown play nodes, such as:
        /// "C.J. Ham 1 Yd Rush, G.Joseph extra point is GOOD, Center-A.DePaola, Holder-R.Wright." or
        /// "Justin Jefferson Pass From Kirk Cousins for 8 Yds Greg Joseph Made Ex. Pt"
        /// "Deon Jackson Pass From Matt Ryan for 1 Yd, C.McLaughlin extra point is GOOD, Center-L.Rhodes, Holder-M.Haack." (this will work for both WR/RB and QB) or
        /// "Tyreek Hill 57 Yd Fumble Recovery (Jason Sanders Kick)" for an offensive fumble recovery for a TD,
        /// find the first occurence (which is the only occurence) of an integer and return that.
        /// </summary>
        /// <param name="touchdownText"></param>
        /// <returns></returns>
        private int GetTouchdownPlayYardage(string touchdownText)
        {
            // regardless of the play, we need to get the yardage
            string[] wordsInTouchdownText = touchdownText.Split(" ");

            // get the integer in this string, which will be the yardage of the play
            int touchdownPlayYardage = -1;
            foreach (var word in wordsInTouchdownText)
            {
                // if the "word" is an integer, this will pass and we'll have our yardage
                if (int.TryParse(word, out int n))
                {
                    touchdownPlayYardage = int.Parse(word);
                    break;
                }
            }

            return touchdownPlayYardage;
        }

        /// <summary>
        /// Given a play string from the play nodes, such as:
        /// "(9:12 - 1st) J.Conner up the middle to ARZ 27 for 23 yards (Ma.Jones)." or 
        /// "(10:21 - 4th) (Shotgun) K.Cousins pass short middle to D.Cook to MIN 26 for 13 yards (J.Blackmon; B.Okereke)."
        /// </summary>
        /// <param name="playText">Text of the play we are pulling the yards from</param>
        /// <returns>The yardage of the given play</returns>
        private int GetPlayYardage(string playText)
        {
            int yards = 0;

            // when there is no gain on the play, there will not be the word "yards", so we need to check for that
            // "(9:37 - 2nd) I.Pacheco up the middle to KC 22 for no gain (M.Addison). "
            if (playText.ToLower().Contains("yards"))
            {
                // get the substring between the words "for" and "yards"
                int indexOfFor = playText.IndexOf(" for ");
                int indexOfYards = playText.IndexOf("yards");

                string strYards = playText.Substring(indexOfFor + " for ".Length, indexOfYards - 1 - (indexOfFor + " for ".Length));

                yards = int.Parse(strYards);
            }

            return yards;
        }

        /// <summary>
        /// Gets the name of the player who caught a pass during a big play. The format of the text is:
        ///   "(10:21 - 4th) (Shotgun) K.Cousins pass short middle to D.Cook to MIN 26 for 13 yards (J.Blackmon; B.Okereke)."
        ///   "(1:24 - 2nd) (Shotgun) J.Herbert pass deep right to J.Palmer ran ob at MIA 9 for 18 yards."
        /// </summary>
        /// <param name="bigPlayText"></param>
        /// <returns></returns>
        private string GetReceivingPlayerNameInPlay(string bigPlayText)
        {
            string receivingPlayer = "";

            int indexOfToWord = bigPlayText.IndexOf("to");

            // get the string starting at the receivers name so we can pull out the name of this player
            string truncatedPlayString = bigPlayText.Substring(indexOfToWord + 1 + ("to".Length));
            receivingPlayer = truncatedPlayString.Substring(0, truncatedPlayString.IndexOf(" "));

            return receivingPlayer;
        }

        /// <summary>
        /// When a player either receives or throws a touchdown, we need to get the name of the player who
        /// threw the touchdown. There are two formats for this:
        ///   "George Kittle Pass From Brock Purdy for 28 Yds, R.Gould extra point is GOOD, Center-T.Pepper, Holder-M.Wishnowsky."
        ///   "Tyreek Hill 60 Yd pass from Tua Tagovailoa (Jason Sanders Kick)" (this will work for both WR/RB and QB) or
        /// with the first one being what should be seen during a live game and the second one is what that first one is changed
        /// to once the game is over. So technically, since this is run only during live games, we should only care about the
        /// first format of the string.
        /// </summary>
        /// <param name="touchdownText">The text of the touchdown play</param>
        /// <returns></returns>
        private string GetPassingPlayerNameInTouchdown(string touchdownText)
        {
            string passingPlayer = "";

            // We're first checking if there is not a left paren, which should only be there for a completed game
            if (touchdownText.IndexOf("(") == -1)
            {
                // the name of the player who threw the TD is between the word "From" and the word "for"
                int indexOfWordFrom = touchdownText.IndexOf("From");
                int indexOfWordFor = touchdownText.IndexOf("for");

                // In some rare cases, when a game ends, the kicker isn't listed in parens, so we can just check if the word "for" doesn't exist
                // Since this is for an ended game, this shouldn't matter, but we'll check just in case and to support local test
                if (indexOfWordFor == -1)
                {
                    passingPlayer = touchdownText.Substring(indexOfWordFrom + ("From".Length + 1)).Trim();
                }
                else
                {
                    passingPlayer = touchdownText.Substring(indexOfWordFrom + ("From".Length + 1), (indexOfWordFor - (indexOfWordFrom + "From".Length + 2)));
                }
            }
            else if (touchdownText.IndexOf("(") != -1)
            {
                // the name of the player who threw the TD is between the word "from" and the first left paren
                int indexOfWordFrom = touchdownText.IndexOf("from");
                int indexOfFirstLeftParenthesis = touchdownText.IndexOf("(");

                passingPlayer = touchdownText.Substring(indexOfWordFrom + ("from".Length + 1), (indexOfFirstLeftParenthesis - (indexOfWordFrom + "from".Length + 2)));
            }

            return passingPlayer;
        }

        /// <summary>
        /// Gets the quarter of the play based on the playText, such as:
        /// 1st quarter: "(9:59 - 1st) 
        /// 2nd quarter: "(11:50 - 2nd)
        /// 3rd quarter: "(14:12 - 3rd)
        /// 4th quarter: "(10:42 - 4th)
        /// OT: "(10:00 - OT) (we'll use the number 5 for OT)
        /// </summary>
        /// <param name="playText">The text of the play</param>
        /// <returns></returns>
        private int GetQuarter(string playText)
        {
            int quarter;

            int indexOfDash = playText.IndexOf("-");
            string strQuarter = playText.Substring(indexOfDash + 2, playText.IndexOf(")") - (indexOfDash + 2));

            switch (strQuarter)
            {
                case "1st":
                    quarter = 1;
                    break;

                case "2nd":
                    quarter = 2;
                    break;

                case "3rd":
                    quarter = 3;
                    break;

                case "4th":
                    quarter = 4;
                    break;

                case "OT":
                    quarter = 5;
                    break;

                default:
                    quarter = -1;
                    break;
            }

            return quarter;
        }

        /// <summary>
        /// Gets the PlayDetails (player) of the defense that scored a defensive touchdown based on the teamId
        /// present in the scoring play token, which we use to get the team name (such as Indianapolis Colts).
        /// </summary>
        /// <param name="playByPlayJsonObject">The JSON object of the play by play</param>
        /// <param name="scoringPlayToken">The scoring play token for a defensive TD</param>
        /// <param name="playersInGame">The list of players in the current game being parsed</param>
        /// <returns></returns>
        private PlayDetails GetDefenseWhoScoredTouchdown(JToken playByPlayJsonObject, JToken scoringPlayToken, List<PlayDetails> playersInGame)
        {
            // the team name (such as Indianapolis Colts) of the team which just scored the defensive TD
            string teamNameOfScoringDefense = "";

            // grab the teamId from this node
            int teamId = int.Parse(((JValue)scoringPlayToken.SelectToken("teamId")).Value.ToString());

            // find the team abbreviation for this ID
            JToken teamTokens = playByPlayJsonObject.SelectToken("page.content.gamepackage.gmStrp.tms");

            foreach (var teamToken in teamTokens)
            {
                int id = int.Parse(((JValue)teamToken.SelectToken("id")).Value.ToString());

                // this is the team which scored the TD, so grab the team abbreviation and break out of the loop
                if (id == teamId)
                {
                    teamNameOfScoringDefense = (string)((JValue)teamToken.SelectToken("displayName")).Value;
                    break;
                }
            }

            // check to see if this defense exists in the players in the game
            var playDetails = playersInGame.SingleOrDefault(x => x.PlayerName.ToLower().Equals(teamNameOfScoringDefense.ToLower()));

            return playDetails;
        }

        /// <summary>
        /// Send the play details as a message to the service bus' play queue.
        /// </summary>
        /// <param name="playDetails">The details of the particular play</param>
        /// <returns></returns>
        private async Task sendPlayMessage(PlayDetails playDetails)
        {
            try
            {
                // service bus namespace
                string serviceBusNamespace = "fantasyfootballstattracker.servicebus.windows.net";

                // Service Bus queue name
                string queueName = "touchdownqueue";

                // the client that owns the connection and can be used to create senders and receivers
                ServiceBusClient client = new ServiceBusClient(serviceBusNamespace, new DefaultAzureCredential());

                // the sender used to publish messages to the queue
                ServiceBusSender sender = client.CreateSender(queueName);

                try
                {
                    ServiceBusMessage message = new ServiceBusMessage(JsonConvert.SerializeObject(playDetails));

                    await sender.SendMessageAsync(message);

                    _logger.LogInformation($"Successfully sent message to Service Bus for player: {playDetails.PlayerName}");
                }
                finally
                {
                    // Calling DisposeAsync on client types is required to ensure that network
                    // resources and other unmanaged objects are properly cleaned up.
                    await sender.DisposeAsync();
                    await client.DisposeAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex.ToString());
            }
        }

        /// <summary>
        /// Gets the SQL Access token so we can connect to the database
        /// </summary>
        /// <returns></returns>
        private static string GetAzureSqlAccessToken()
        {
            // See https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/services-support-managed-identities#azure-sql
            var tokenRequestContext = new TokenRequestContext(new[] { "https://database.windows.net//.default" });
            var tokenRequestResult = new DefaultAzureCredential().GetToken(tokenRequestContext);

            return tokenRequestResult.Token;
        }

        /// <summary>
        /// Updates the TouchdownDetails table with a particular occurence of a touchdown. This touchdown has not already been
        /// parsed for this game.
        /// </summary>
        /// <param name="espnGameId">Live game ID</param>
        /// <param name="touchdownQuarter">The quarter this touchdown occurs</param>
        /// <param name="touchdownGameClock">The game clock when this touchdown occured</param>
        /// <param name="playerName">The player who scored the touchdown</param>
        /// <param name="log">The logger.</param>
        /// <returns></returns>
        private bool AddTouchdownDetails(int espnGameId, int touchdownQuarter, string touchdownGameClock, string playerName, int season, int ownerId, string opponentAbbreviation, DateTime gameDate, ILogger log)
        {
            bool touchdownAdded = false;

            var connectionStringBuilder = new SqlConnectionStringBuilder
            {
                DataSource = "tcp:playersandschedulesdetails.database.windows.net,1433",
                InitialCatalog = "PlayersAndSchedulesDetails",
                TrustServerCertificate = false,
                Encrypt = true
            };

            SqlConnection sqlConnection = new SqlConnection(connectionStringBuilder.ConnectionString);

            try
            {
                string azureSqlToken = GetAzureSqlAccessToken();
                sqlConnection.AccessToken = azureSqlToken;
            }
            catch (Exception e)
            {
                log.LogInformation(e.Message);
            }

            // Get current EST time - If this is run on a machine with a differnet local time, DateTime.Now will not return the proper time
            TimeZoneInfo easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
            DateTime currentEasterStandardTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, easternZone);

            using (sqlConnection)
            {
                sqlConnection.Open();

                // call stored procedure to add this touchdown for this player to the database if it hasn't already
                // been added
                using (SqlCommand command = new SqlCommand("AddTouchdownAlertDetails", sqlConnection))
                {
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    command.Parameters.Add(new SqlParameter("@EspnGameId", System.Data.SqlDbType.Int) { Value = espnGameId });
                    command.Parameters.Add(new SqlParameter("@TouchdownQuarter", System.Data.SqlDbType.Int) { Value = touchdownQuarter });
                    command.Parameters.Add(new SqlParameter("@TouchdownGameClock", System.Data.SqlDbType.NVarChar) { Value = touchdownGameClock });
                    command.Parameters.Add(new SqlParameter("@PlayerName", System.Data.SqlDbType.NVarChar) { Value = playerName });
                    command.Parameters.Add(new SqlParameter("@Season", System.Data.SqlDbType.Int) { Value = season });
                    command.Parameters.Add(new SqlParameter("@OpponentAbbreviation", System.Data.SqlDbType.NVarChar) { Value = opponentAbbreviation });
                    command.Parameters.Add(new SqlParameter("@GameDate", System.Data.SqlDbType.DateTime) { Value = gameDate });
                    command.Parameters.Add(new SqlParameter("@OwnerID", System.Data.SqlDbType.Int) { Value = ownerId });
                    command.Parameters.Add(new SqlParameter("@TouchdownTimeStamp", System.Data.SqlDbType.DateTime) { Value = currentEasterStandardTime });

                    touchdownAdded = (bool)command.ExecuteScalar();
                }

                sqlConnection.Close();
            }

            return touchdownAdded;
        }

        /// <summary>
        /// Updates the BigPlayDetails table with a particular occurence of a touchdown. This touchdown has not already been
        /// parsed for this game.
        /// </summary>
        /// <param name="espnGameId">Live game ID</param>
        /// <param name="quarter">The quarter this touchdown occurs</param>
        /// <param name="gameClock">The game clock when this touchdown occured</param>
        /// <param name="playerName">The player who scored the touchdown</param>
        /// <param name="log">The logger.</param>
        /// <returns></returns>
        private bool AddBigPlayDetails(int espnGameId, int quarter, string gameClock, string playerName, int season, int ownerId, string opponentAbbreviation, DateTime gameDate, ILogger log)
        {
            bool bigPlayAdded = false;

            var connectionStringBuilder = new SqlConnectionStringBuilder
            {
                DataSource = "tcp:playersandschedulesdetails.database.windows.net,1433",
                InitialCatalog = "PlayersAndSchedulesDetails",
                TrustServerCertificate = false,
                Encrypt = true
            };

            SqlConnection sqlConnection = new SqlConnection(connectionStringBuilder.ConnectionString);

            try
            {
                string azureSqlToken = GetAzureSqlAccessToken();
                sqlConnection.AccessToken = azureSqlToken;
            }
            catch (Exception e)
            {
                log.LogInformation(e.Message);
            }

            // Get current EST time - If this is run on a machine with a differnet local time, DateTime.Now will not return the proper time
            TimeZoneInfo easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
            DateTime currentEasterStandardTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, easternZone);

            using (sqlConnection)
            {
                sqlConnection.Open();

                // call stored procedure to add this touchdown for this player to the database if it hasn't already
                // been added
                using (SqlCommand command = new SqlCommand("AddBigPlayAlertDetails", sqlConnection))
                {
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    command.Parameters.Add(new SqlParameter("@EspnGameId", System.Data.SqlDbType.Int) { Value = espnGameId });
                    command.Parameters.Add(new SqlParameter("@BigPlayQuarter", System.Data.SqlDbType.Int) { Value = quarter });
                    command.Parameters.Add(new SqlParameter("@BigPlayGameClock", System.Data.SqlDbType.NVarChar) { Value = gameClock });
                    command.Parameters.Add(new SqlParameter("@PlayerName", System.Data.SqlDbType.NVarChar) { Value = playerName });
                    command.Parameters.Add(new SqlParameter("@Season", System.Data.SqlDbType.Int) { Value = season });
                    command.Parameters.Add(new SqlParameter("@OpponentAbbreviation", System.Data.SqlDbType.NVarChar) { Value = opponentAbbreviation });
                    command.Parameters.Add(new SqlParameter("@GameDate", System.Data.SqlDbType.DateTime) { Value = gameDate });
                    command.Parameters.Add(new SqlParameter("@OwnerID", System.Data.SqlDbType.Int) { Value = ownerId });
                    command.Parameters.Add(new SqlParameter("@BigPlayTimeStamp", System.Data.SqlDbType.DateTime) { Value = currentEasterStandardTime });

                    bigPlayAdded = (bool)command.ExecuteScalar();
                }

                sqlConnection.Close();
            }

            return bigPlayAdded;
        }
    }
}