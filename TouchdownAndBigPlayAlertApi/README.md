# Touchdown and Big Play Alert API

A .NET 8 Web API that parses ESPN NFL game data to detect touchdowns and big plays for fantasy football players and sends alerts via Azure Service Bus.

## Features

- Parses live ESPN NFL game data
- Detects touchdowns and big plays (i.e., 25+ yards receiving/rushing, 40+ yards passing)
- Stores play data in Azure SQL Database
- Sends alerts via Azure Service Bus
- Supports both offensive and defensive touchdowns

## Prerequisites

- .NET 8 SDK
- Azure SQL Database
- Azure Service Bus
- Azure App Service (for deployment)

## Local Development

1. Clone the repository
2. Update connection strings in `appsettings.json`
3. Run the application