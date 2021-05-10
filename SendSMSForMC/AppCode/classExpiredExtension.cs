using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Windows.Forms;
using System.IO;
using SendSMSForMC.AppCode;
using System.Data;
using System.Data.OracleClient;


namespace SendSMSForMC
{
    class classExpiredExtension
    {
        public static bool exitThread = false;
        public static string _updateDateTime = null;

        private static classDataAccess _dataAccess = new classDataAccess();

        public static string SMS_TYPE = "EXCARD";

        private static string SCBPhone = "";

        public static void RunService()
        {
            int hour = 0;
            int minute = 0;
            int dayValue1 = 0;            
            int hourValue = 0;
            int minuteValue = 0;
            DataTable table = new DataTable();

            SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            dayValue1 = classUtilities.GetIntValueFromConfig("Expired_Extension_Day");
            //dayValue2 = classUtilities.GetIntValueFromConfig("Expired_Extension_Day2");
            hourValue = classUtilities.GetIntValueFromConfig("Expired_Extension_Hour");
            minuteValue = classUtilities.GetIntValueFromConfig("Expired_Extension_Minute");

            DateTime today = DateTime.Today;
            DateTime date1_p = new DateTime(today.Year, today.Month, 1).AddDays(dayValue1);

            //DateTime date1_p = new DateTime(today.Year, today.Month, 1).AddDays(dayValue1);
            //DateTime date2_p = new DateTime(today.Year, today.Month, DateTime.DaysInMonth(today.Year, today.Month)).AddDays(dayValue2);

            //DateTime date3_p = date2_p.AddDays(38);



            while (exitThread == false)
            {
                try
                {
                    DateTime day_p = DateTime.Today;
                    hour = DateTime.Now.Hour;
                    minute = DateTime.Now.Minute;
                    if (day_p == date1_p)
                    //if (1==1)//hhhh
                    {
                        if (hour == hourValue && minute == minuteValue)
                        //if (1 == 1)//hhhh
                        {

                            classExpiredExtensionLogWriter.WriteLog("----------------Begin Process-----------------");
                            _dataAccess = new classDataAccess();
                            table.Rows.Clear();
                            table = Get_Expired_Extension();
                            //table = GetExpiredExtensionBoth();
                            if (table.Rows.Count > 0)
                                Insert_SMSMessage(table);

                            classExpiredExtensionLogWriter.WriteLog("----------------End Process-----------------");

                            if (day_p == date1_p)
                            {
                                classExpiredExtensionLogWriter.WriteLog("(day_p == date1_p) sleep(day): " + 24);
                                int temp = (1000 * (60 * 60)*24);
                                Thread.Sleep(temp * 23);//sleep 23 ngay vi qua 24 ngay vuot kieu int
                            }

                            Thread.Sleep(1000 * 60);// de khong lap lap lai vong lap lan 2 trong ngay


                        }
                        else
                        
                        {
                            if ((hour == hourValue && minute > minuteValue) || (hour > hourValue))// da qua gio thuc thi, sleep de chuyen den ngay tiep theo
                            {
                                classExpiredExtensionLogWriter.WriteLog("((hour == hourValue && minute > minuteValue) || (hour > hourValue)) sleep(hour): " + (24 - (hour - hourValue + 1)));
                                Thread.Sleep(1000 * (60 * 60) * (24 - (hour - hourValue + 1)));

                            }
                            else
                            {
                                if (hour < (hourValue -1))
                                {
                                    classExpiredExtensionLogWriter.WriteLog("(hour < hourValue) sleep(miute): " + ((hourValue - hour - 1) * 60 - minute));
                                    Thread.Sleep(1000 * 60 * ((hourValue-hour -1)*60 - minute + minuteValue -1));
                                }
                                else
                                {
                                    if (hour == (hourValue - 1))
                                    {
                                        classExpiredExtensionLogWriter.WriteLog("(hour == (hourValue - 1)) sleep(minute): " + (60 - minute + minuteValue - 1));
                                        Thread.Sleep((1000 * 60) * (60 - minute + minuteValue - 1));
                                        if((60 - minute + minuteValue - 1) ==0)//1 phut truoc gio gui tn
                                            Thread.Sleep(1000 * 10);
                                    }
                                    else//(hour==hourValue)
                                    {
                                        if (minute < (minuteValue - 1))
                                        {
                                            classExpiredExtensionLogWriter.WriteLog("(minute <= (minuteValue - 1)) sleep(minute): " + (minuteValue - minute - 1));
                                            Thread.Sleep((1000 * 60)*(minuteValue -minute -1));
                                        }
                                        else
                                        {
                                            classExpiredExtensionLogWriter.WriteLog("sleep(ss): " + 10);
                                            Thread.Sleep(1000 * 10);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        //if (day_p < date1_p.AddDays(-1) || day_p > date1_p)
                        if (day_p < date1_p.AddDays(-2) || day_p > date1_p)
                        {
                            classExpiredExtensionLogWriter.WriteLog("day_p < (date1 - 1) || day_p > date1_p: sleep 1 day");
                            Thread.Sleep(1000 * (60 * 60) * 24);
                        }
                        //////////////hhhh
                        if (day_p == date1_p.AddDays(-2))
                        {
                            if (hour < (hourValue - 1))
                            {
                                classExpiredExtensionLogWriter.WriteLog("day_p = date1_p.AddDays(-2): sleep (hour)" + (hour - (hourValue - 1)));
                                Thread.Sleep(1000 * (60 * 60) * (hour - (hourValue - 1)));
                            }
                            else
                            {
                                classExpiredExtensionLogWriter.WriteLog("day_p = date1_p.AddDays(-2): sleep 1 hour" );
                                Thread.Sleep(1000 * (60 * 60));
                            }

                        }
                        ///////
                        if (day_p == date1_p.AddDays(-1))
                        {
                            if (hour >= hourValue)
                            {
                                classExpiredExtensionLogWriter.WriteLog("(hour >= hourValue) sleep(minute): " + (60 * (24 - (hour - hourValue + 1))));
                                Thread.Sleep(1000 * (60 * 60) * (24 - hour + hourValue - 1));
                            }
                            if (hour < hourValue)
                            {
                                classExpiredExtensionLogWriter.WriteLog("(hour <= hourValue) sleep(minute): " + (60 * (24 + (hourValue - hour - 1))));
                                Thread.Sleep(1000 * (60 * 60) * (24 + (hourValue - hour - 1)));
                            }
                        }   
                    
                    }
                    
                }
                catch (Exception ex)
                {
                    classExpiredExtensionLogWriter.WriteLog("Error RunService(), " + ex.Message);
                }
            }
        }
        private static DataTable Get_Expired_Extension()
        {
            DataTable table = new DataTable();
            try
            {
                string currentMonth = DateTime.Now.AddMonths(1).ToString("yyyyMM");
                table = _dataAccess.GetExpiredExtension(currentMonth);
            }
            catch (Exception ex)
            {
                classExpiredExtensionLogWriter.WriteLog("Error Get_Expired_Extension(), " + ex.Message);
            }
            return table;
        }

        private static DataTable GetExpiredExtensionBoth()
        {
            DataTable table = new DataTable();
            try
            {
                table = _dataAccess.GetExpiredExtensionBoth();
            }
            catch (Exception ex)
            {
                classExpiredExtensionLogWriter.WriteLog("Error GetExpiredExtensionBoth(), " + ex.Message);
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
            int count_err = 0;
            //string expiredDate_P = DateTime.Today.Month.ToString()+"/"+DateTime.Today.Year.ToString();
            foreach (DataRow row in table.Rows)
            {
                result = 0;

                message = CreateSMSMessage(row.ItemArray[4].ToString(), row.ItemArray[0].ToString(), row.ItemArray[1].ToString(), row.ItemArray[9].ToString(), row.ItemArray[10].ToString());               

                if (string.IsNullOrEmpty(message) == false)
                {
                    string mobile = classUtilities.GetMobileFromCardNoOfSpecialList(row.ItemArray[0].ToString(), row.ItemArray[2].ToString());
                    if (row.ItemArray[2].ToString() == "khong co")
                    {
                        result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                            row.ItemArray[7].ToString()
                                                                            , mobile //row.ItemArray[2].ToString() //classDataAccess.MYPHONE
                                                                            , message
                                                                            , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
                                                                            , SMS_TYPE);
                    }
                    else
                    {
                        result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                            row.ItemArray[7].ToString()
                                                                            , mobile //row.ItemArray[2].ToString() //classDataAccess.MYPHONE
                                                                            , message
                                                                            , 'N'//Y se ko gui tin nhan, N se gui tin nhan
                                                                            , SMS_TYPE);
                    }
                    if (result == 1)
                    {
                        if (row.ItemArray[2].ToString() == "khong co")
                        {
                            count += dwDataAccess.InsertExpiredExtensionToDW(SMS_TYPE, message
                                                                , row.ItemArray[2].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                                                , DateTime.Now
                                //, row.ItemArray[0].ToString()
                                                                , row.ItemArray[8].ToString()
                                                                , row.ItemArray[4].ToString()
                                                                , row.ItemArray[3].ToString()
                                                                , row.ItemArray[1].ToString()
                                                                , "Y"
                                                                );
                        }
                        else
                        {
                            count += dwDataAccess.InsertExpiredExtensionToDW(SMS_TYPE, message
                                                                , row.ItemArray[2].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                                                , DateTime.Now
                                //, row.ItemArray[0].ToString()
                                                                , row.ItemArray[8].ToString()
                                                                , row.ItemArray[4].ToString()
                                                                , row.ItemArray[3].ToString()
                                                                , row.ItemArray[1].ToString()
                                                                , "N"
                                                                );
                        }
                    }
                    else
                    {
                        count_err += dwDataAccess.InsertExpiredExtensionToDW(SMS_TYPE, message
                                                               , row.ItemArray[2].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                                               , DateTime.Now
                            //, row.ItemArray[0].ToString()
                                                               , row.ItemArray[8].ToString()
                                                               , row.ItemArray[4].ToString()
                                                               , row.ItemArray[3].ToString()
                                                               , row.ItemArray[1].ToString()
                                                               , "E"
                                                               );
                    }
                }
            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            classExpiredExtensionLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + count);
            classExpiredExtensionLogWriter.WriteLog("Message loi khong Insert vao EbankGW: " + count_err);
            return;
        }

        private static string CreateSMSMessage(string brand, string pan, string expiredDate, string vip_card, string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                expiredDate = expiredDate.Substring(4, 2) + "/" + expiredDate.Substring(0, 4);
                //string smsMessage = "The SCB " + brand + " xxxx" + pan + " cua Quy Khach se het hieu luc tu thang " + expiredDate
                  //              + ". Vui long den DGD cua SCB de gia han The hoac lien he " + SCBPhone + " de duoc tu van.";
                string smsMessage = "Cam on Quy khach da su dung the SCB " + brand + " x" + pan + "\nThe se het han vao cuoi thang " + expiredDate
                                + "\nVui long lien he SCB gan nhat hoac goi " +SCBPhone+" de gia han";
                
                if (smsMessage.Length > 160)
                    smsMessage = smsMessage.Substring(0, 160);
                return smsMessage;
            }
            catch (Exception ex)
            {
                classExpiredExtensionLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }
        }
       
    }
}
