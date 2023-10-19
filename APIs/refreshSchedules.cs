using System;
using Amazon.QuickSight;
using Amazon.QuickSight.Model;

namespace TestDotnetSDK
{
    class Program
    {
        private static readonly string AccessKey = "";
        private static readonly string SecretAccessKey = "";
        private static readonly string Token = ""
        private static readonly string AccountID = "";
        private static readonly string DataSetId = "";
        private static readonly string ScheduleId = "";

        static async Task Main(string[] args)
        {
            var awsCredentials = new Amazon.Runtime.SessionAWSCredentials(AccessKey, SecretAccessKey, Token);
            DateTime dateTime = new DateTime(2026, 6, 1);
            var quicksightConfig = new AmazonQuickSightConfig
            {
            };
            var client = new AmazonQuickSightClient(
                awsCredentials,
                quicksightConfig);

            var listRefreshSchedulesRequest = new ListRefreshSchedulesRequest
            {
                AwsAccountId = AccountID,
                DataSetId = DataSetId
            };

            var describeRefreshScheduleRequest = new DescribeRefreshScheduleRequest
            {
                AwsAccountId = AccountID,
                DataSetId = DataSetId,
                ScheduleId = ScheduleId
            };

            var deleteRefreshScheduleRequest = new DeleteRefreshScheduleRequest
            {
                AwsAccountId = AccountID,
                DataSetId = DataSetId,
                ScheduleId = ScheduleId
            };

            var refreshOnDay = new ScheduleRefreshOnEntity{
                DayOfWeek = Amazon.QuickSight.DayOfWeek.MONDAY
            };

          Random random = new Random();
          int randomHour = random.Next(0, 24); // Generate a random hour between 0 and 23
          int randomMinute = random.Next(0, 60); // Generate a random minute between 0 and 59
          string randomTime = $ "{randomHour:D2}:{randomMinute:D2}";
          
            var scheduleFrequency = new RefreshFrequency{
                RefreshOnDay = refreshOnDay,
                Interval = "WEEKLY",
                Timezone = "EST",
                TimeOfTheDay = randomTime
            };

            var scheduleFrequency2 = new RefreshFrequency{
                RefreshOnDay = refreshOnDay,
                Interval = "WEEKLY",
                Timezone = "CST",
                TimeOfTheDay = randomTime
            };

            var refreshSchedule = new RefreshSchedule
            {
                RefreshType = "INCREMENTAL_REFRESH",
                ScheduleId = "Schedule18012023",
                StartAfterDateTime = dateTime,
                ScheduleFrequency = scheduleFrequency
            };

            var refreshSchedule2 = new RefreshSchedule
            {
                RefreshType = "INCREMENTAL_REFRESH",
                ScheduleId = "Schedule18012023",
                StartAfterDateTime = dateTime,
                ScheduleFrequency = scheduleFrequency2
            };

            var createRefreshScheduleRequest = new CreateRefreshScheduleRequest
            {
                AwsAccountId = AccountID,
                DataSetId = DataSetId,
                Schedule = refreshSchedule
            };

            var updateRefreshScheduleRequest = new UpdateRefreshScheduleRequest
            {
                AwsAccountId = AccountID,
                DataSetId = DataSetId,
                Schedule = refreshSchedule2
            };

            try{
                // test create refresh schedule
                CreateRefreshScheduleResponse createResponse = await client.CreateRefreshScheduleAsync(createRefreshScheduleRequest);
                Console.WriteLine(createResponse.ToString());

               /*  // test update refresh schedule
                UpdateRefreshScheduleResponse updateResponse = await client.UpdateRefreshScheduleAsync(updateRefreshScheduleRequest);
                Console.WriteLine(updateResponse.ToString());
 */
                // test list refresh schedules
                client.ListRefreshSchedulesAsync(listRefreshSchedulesRequest).Result.RefreshSchedules.ForEach(
                schedule => Console.WriteLine(schedule.Arn)
                );

                // test describe refresh schedule
                DescribeRefreshScheduleResponse response = await client.DescribeRefreshScheduleAsync(describeRefreshScheduleRequest);
                Console.WriteLine(response.RefreshSchedule.ScheduleFrequency.Timezone);

                /* // test delete refresh schedule
                DeleteRefreshScheduleResponse deleteResponse = await client.DeleteRefreshScheduleAsync(deleteRefreshScheduleRequest);
                Console.WriteLine(deleteResponse.ToString()); */

                // test list refresh schedules
                client.ListRefreshSchedulesAsync(listRefreshSchedulesRequest).Result.RefreshSchedules.ForEach(
                schedule => Console.WriteLine(schedule.Arn)
                );
            } catch(Exception ex){
                Console.WriteLine("Exception : " + ex.ToString());
            }
        }
    }
}
