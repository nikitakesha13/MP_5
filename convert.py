import xlwt
from xlwt import Workbook

wb = Workbook()

OftwenTweets = wb.add_sheet("Time Of Tweets")
SleepTime = wb.add_sheet("Time of Sleep")

often = open("OftenTweets.txt", 'r')
sleep = open("SleepTime.txt", 'r')

OftenLines = often.readlines()
SleepLines = sleep.readlines()

for i in range(24):
    often_arr = OftenLines[i].split("   ")
    OftwenTweets.write(i, 0, often_arr[0])
    OftwenTweets.write(i, 1, int(often_arr[1]))

    sleep_arr = SleepLines[i].split("   ")
    SleepTime.write(i, 0, sleep_arr[0])
    SleepTime.write(i, 1, int(sleep_arr[1]))

wb.save("TweetsReport.xls")
