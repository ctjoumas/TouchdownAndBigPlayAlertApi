namespace TouchdownAndBigPlayAlertApi
{
    /// <summary>
    /// This class represents the different types of messages we will send to an owner for their player.
    /// Currently, this includes either a touchdown or a specific big play their player made.
    /// </summary>
    public class PlayDetails
    {
        public int Season { get; set; }
        public int OwnerId { get; set; }
        public string? OwnerName { get; set; }
        public string? PlayerName { get; set; }
        public string? PlayerPosition { get; set; }
        public string? PhoneNumber { get; set; }
        public string? TeamAbbreviation { get; set; }
        public string? OpponentAbbreviation { get; set; }
        public DateTime GameDate { get; set; }
        public string? Message { get; set; }
    }
}