

using Reading.BigData.Coursework;

var cancelationSource = new CancellationTokenSource();
var jobName = Environment.GetCommandLineArgs()[1];
var inputPath = Environment.GetCommandLineArgs()[2];

// dotnet run AIRPORT .\Data\AComp_Passenger_data_no_error.csv
if (jobName == "AIRPORT")
{
    var numberOfFlightPerAirportsJob = new NumberOfFlightsPerAirports().Create(inputPath, inputPath.Replace(".csv", "_Airports_Flights_FINAL.csv"), cancelationSource.Token);
    numberOfFlightPerAirportsJob.Run();
}

// dotnet run PASSENGERS .\Data\AComp_Passenger_data_no_error.csv
if (jobName == "PASSENGERS")
{
    var passengersWithHighestNumberOfFlightsStepOne = new PassengersWithHighestNumberOfFlightsStepOne().Create(inputPath, inputPath.Replace(".csv", "_Passenge_Highest_Flights_Number_TEMP.csv"), cancelationSource.Token);
    passengersWithHighestNumberOfFlightsStepOne.Run();

    var passengersWithHighestNumberOfFlightsStepTwo = new PassengersWithHighestNumberOfFlightsStepTwo().Create(inputPath.Replace(".csv", "_Passenge_Highest_Flights_Number_TEMP.csv"), inputPath.Replace(".csv", "_Passenge_Highest_Flights_Number_FINAL.csv"), cancelationSource.Token);
    passengersWithHighestNumberOfFlightsStepTwo.Run();
}