using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Windows.Forms;
using System.Data;
using SendSMSForMC.AppCode;

namespace SendSMSForMC
{
    class classAccumulativeRewardPoint
    {
        public static bool exitThread = false;

        private static classDataAccess _dataAccess = new classDataAccess();

        public static string SMS_TYPE = "REWARD";

        private static string SCBPhone = "";

        public static void RunService()
        {
            int day = 0;
            int hour = 0;
            int minute = 0;
            int dayValue = 0;
            int hourValue = 0;
            int minuteValue = 0;
            DataTable table = new DataTable();

            SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            dayValue = classUtilities.GetIntValueFromConfig("Accumulative_RewardPoint_Day");
            hourValue = classUtilities.GetIntValueFromConfig("Accumulative_RewardPoint_Hour");
            minuteValue = classUtilities.GetIntValueFromConfig("Accumulative_RewardPoint_Minute");

            while (exitThread == false)
            {
                day = DateTime.Now.Day;
                hour = DateTime.Now.Hour;
                minute = DateTime.Now.Minute;

                if (day == dayValue && hour == hourValue && minute == minuteValue)
                {
                    try
                    {
                        classAccumulativeRewardPointLogWriter.WriteLog("----------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();
                        table.Rows.Clear();
                        table = Get_Reward_Point();
                        if (table.Rows.Count > 0)
                            Insert_SMSMessage(table);

                        classAccumulativeRewardPointLogWriter.WriteLog("----------------End Process-----------------");
                        Thread.Sleep(1000 * 3600 * 24 * 23);// sleep 25 ngay sau khi thuc hien xong
                    }
                    catch (Exception ex)
                    {
                        classAccumulativeRewardPointLogWriter.WriteLog("Error RunService(), " + ex.Message);
                    }
                }
                Thread.Sleep(1000 * 50); // 1 phut moi check 1 lan
            }
        }

        private static DataTable Get_Reward_Point()
        {
            DataTable table = new DataTable();
            try
            {
                table = _dataAccess.GetRewardPoint();
            }
            catch (Exception ex)
            {
                classAccumulativeRewardPointLogWriter.WriteLog("Error Get_Reward_Point(), " + ex.Message);
            }
            return table;
        }

        private static void Insert_SMSMessage(DataTable table)
        {
            classDataAccess ebankDataAccess = new classDataAccess();
            classDataAccess dwDataAccess = new classDataAccess();
            ebankDataAccess.OpenConnection("EBANK_GW");
            dwDataAccess.OpenConnection("CW_DW");

            string message = "";
            int result = 0;
            int count = 0;

            //OracleCommand cmd = ebankDataAccess.AddProcedureParameterToEBankGW();

            foreach (DataRow row in table.Rows)
            {
                result = 0;
                message = CreateSMSMessage(row.ItemArray[5].ToString(), row.ItemArray[0].ToString()
                                            , row.ItemArray[1].ToString(), row.ItemArray[2].ToString());
                if (string.IsNullOrEmpty(message) == false)
                {
                    if (row.ItemArray[3].ToString() == "khong co")
                    {
                        result = ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                                            , row.ItemArray[3].ToString() //classDataAccess.MYPHONE
                                                                            , message
                                                                            , 'Y'//Sent (se ko gui tin nhan)
                                                                            , SMS_TYPE);
                    }
                    else
                    {
                        result = ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                                            , row.ItemArray[3].ToString() //classDataAccess.MYPHONE
                                                                            , message
                                                                            , 'N'//Sent (se ko gui tin nhan)
                                                                            , SMS_TYPE);
                    }
                    if (result == 1)
                    {
                        if (row.ItemArray[3].ToString() == "khong co")
                        {
                            count += dwDataAccess.InsertRewardPointToDW(SMS_TYPE, message
                                                                , row.ItemArray[3].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                                                , DateTime.Parse(row.ItemArray[6].ToString())
                                                                , row.ItemArray[0].ToString()
                                                                , row.ItemArray[5].ToString()
                                                                , row.ItemArray[4].ToString()
                                                                , row.ItemArray[2].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                , row.ItemArray[1].ToString()
                                                                , "Y"
                                                                );
                        }
                        else
                        {
                            count += dwDataAccess.InsertRewardPointToDW(SMS_TYPE, message
                                                              , row.ItemArray[3].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                                              , DateTime.Parse(row.ItemArray[6].ToString())
                                                              , row.ItemArray[0].ToString()
                                                              , row.ItemArray[5].ToString()
                                                              , row.ItemArray[4].ToString()
                                                              , row.ItemArray[2].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                              , row.ItemArray[1].ToString()
                                                              , "N"
                                                              );
                        }
                    }
                }

            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            classAccumulativeRewardPointLogWriter.WriteLog("So luong message da duoc Insert vao EbankGW thanh cong: " + count);
            return;
        }

        private static string CreateSMSMessage(string brand, string pan, string pointMonth, string point)
        {
            try
            {
                pan = pan.Substring(12, 4);

                pointMonth = "25/" + pointMonth.Substring(0, 2) + "/" + pointMonth.Substring(2, 4);
                long pnt = long.Parse(point);
                point = string.Format("{0:#,##0}", pnt);

                string smsMessage = "DIEM THUONG TICH LUY CUA THE " + brand + " xx" + pan + " DEN HET " + pointMonth
                                + " LA " + point + " DIEM. CHI TIET VUI LONG LIEN HE " + SCBPhone + ".";
                if (smsMessage.Length > 160)
                    smsMessage = smsMessage.Substring(0, 160);
                return smsMessage;
            }
            catch (Exception ex)
            {
                classAccumulativeRewardPointLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }
        }
    }
}
