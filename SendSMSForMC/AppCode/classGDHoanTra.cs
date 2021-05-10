using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Windows.Forms;
using System.Data;
using SendSMSForMC.AppCode;

namespace SendSMSForMC.AppCode
{
    class classGDHoanTra
    {

        public static bool _exitThread = false;
        public static string _updateDateTime = null;

        private static classDataAccess _dataAccess = new classDataAccess();

        public static string SMS_TYPE = "GDHT";

        private static string SCBPhone = "";

        public static void RunService()
        {
            int hour = 0;
            int minute = 0;
            
            int hourValue = 0;
                int minuteValue = 0;
            DataTable table = new DataTable();

            SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            hourValue = classUtilities.GetIntValueFromConfig("Accumulative_GDHoanTra_Hour");
            minuteValue = classUtilities.GetIntValueFromConfig("Accumulative_GDHoanTra_Minute");

            while (_exitThread == false)
            {
                try
                {
                    hour = DateTime.Now.Hour;
                    minute = DateTime.Now.Minute;

                    if (hour == hourValue && minute == minuteValue)
                    //if(1==1)//hhhh
                    {

                        classGDHoanTraLogWriter.WriteLog("----------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();
                        table.Rows.Clear();
                        table = Get_GDHoanTra();
                        if (table.Rows.Count > 0)
                            Insert_SMSMessage(table);

                        classGDHoanTraLogWriter.WriteLog("----------------End Process-----------------");
                        
                        Thread.Sleep(1000 * 60);// de khong lap lap lai vong lap lan 2 trong ngay


                    }
                    else
                    {
                        
                        if ((hour == hourValue && minute > minuteValue) || (hour > hourValue))// da qua gio thuc thi, sleep de chuyen den ngay tiep theo
                        {
                            classGDHoanTraLogWriter.WriteLog("((hour == hourValue && minute > minuteValue) || (hour > hourValue)) sleep(minute): " + (60 * (24 - (hour - hourValue + 1))));
                            Thread.Sleep(1000 * (60 * 60) * (24 - (hour - hourValue + 1)));

                        }
                        else
                        {
                            if (hour < (hourValue - 1))
                            {
                                classGDHoanTraLogWriter.WriteLog("(hour < hourValue) sleep(miute): " + ((hourValue - hour - 1) * 60 - minute + minuteValue - 1));
                                Thread.Sleep(1000 * 60 * ((hourValue - hour - 1) * 60 - minute + minuteValue - 1));
                            }
                            else
                            {
                                if (hour == (hourValue - 1))
                                {
                                    classGDHoanTraLogWriter.WriteLog("(hour == (hourValue - 1)) sleep(minute): " + (60 - minute + minuteValue - 1));
                                    Thread.Sleep((1000 * 60) * (60 - minute + minuteValue - 1));
                                    if ((60 - minute + minuteValue - 1) == 0)//1 phut truoc gio gui tn
                                        Thread.Sleep(1000 * 10);
                                }
                                else//(hour==hourValue)
                                {
                                    if (minute < (minuteValue - 1))
                                    {
                                        classGDHoanTraLogWriter.WriteLog("(minute <= (minuteValue - 1)) sleep(minute): " + (minuteValue - minute - 1));
                                        Thread.Sleep((1000 * 60) * (minuteValue - minute - 1));
                                    }
                                    else
                                    {
                                        classGDHoanTraLogWriter.WriteLog("sleep(ss): " + 10);
                                        Thread.Sleep(1000 * 10);
                                    }
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    classGDHoanTraLogWriter.WriteLog("Error RunService(), " + ex.Message);
                }
            }
        }

        private static DataTable Get_GDHoanTra()
        {
            string maxUpdateDT = null;
            DataTable table = new DataTable();
            if (string.IsNullOrEmpty(_updateDateTime) == false)
            {
                maxUpdateDT = _updateDateTime;
                _updateDateTime = null;
            }
            else
            {
                maxUpdateDT = _dataAccess.Get_Max_UpdateTime(SMS_TYPE);
                //maxUpdateDT = _dataAccess.Get_Max_UpdateTime_Err(SMS_TYPE);
                //DataTable updateTime = _dataAccess.Get_Max_OutBal_UpdateTime(SMS_TYPE);
                //if (updateTime.Rows.Count == 1 || updateTime.Rows.Count == 2)
                //    maxUpdateDT = updateTime.Rows[0].ItemArray[0].ToString();
            }
            if (string.IsNullOrEmpty(maxUpdateDT) == false)
            {
                try
                {
                    long maxUpdateTime = long.Parse(maxUpdateDT);
                }
                catch (Exception ex)
                {
                    classGDHoanTraLogWriter.WriteLog("Error Get_GDHoanTra(), " + ex.Message);
                    return null;
                }
                if (maxUpdateDT != null)
                    table = _dataAccess.GetGDHoanTra(maxUpdateDT);
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
            //OracleCommand cmd = ebankDataAccess.AddProcedureParameterToEBankGW();           l

            foreach (DataRow row in table.Rows)
            {
                result = 0;      

                message = CreateSMSMessage(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(),
                                                        row.ItemArray[6].ToString(), row.ItemArray[7].ToString(),
                                                        row.ItemArray[8].ToString(), row.ItemArray[9].ToString()
                                                        ,row.ItemArray[17].ToString(),row.ItemArray[18].ToString()
                                                        );
                if (string.IsNullOrEmpty(message) == false)
                {
                    
                    
                    string mobile = classUtilities.GetMobileFromCardNoOfSpecialList(row.ItemArray[4].ToString(), row.ItemArray[10].ToString());
                    if (row.ItemArray[10].ToString() == "khong co")
                    {
                        result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                            row.ItemArray[15].ToString()
                                                                            , mobile
                                                                            , message
                                                                            , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
                                                                            , SMS_TYPE);
                    }
                    else
                    {
                        result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                             row.ItemArray[15].ToString()
                                                                            , mobile
                                                                            , message
                                                                            , 'N'//Y se ko gui tin nhan, N se gui tin nhan
                                                                            , SMS_TYPE);
                    }
                    if (result == 1)
                    {

                        if (row.ItemArray[10].ToString() == "khong co")
                        {
                            count += dwDataAccess.InsertGDHoanTraSMSToDW(
                                                                SMS_TYPE, message
                                                                , mobile //    ,long.Parse(row.ItemArray[10].ToString())
                                                                , DateTime.Parse(row.ItemArray[0].ToString())
                                //, row.ItemArray[4].ToString()
                                                                , row.ItemArray[16].ToString()
                                                                , row.ItemArray[2].ToString()
                                                                , row.ItemArray[3].ToString()
                                                                , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                                , row.ItemArray[9].ToString()
                                                                , row.ItemArray[11].ToString()
                                                                , row.ItemArray[12].ToString()
                                                                , row.ItemArray[14].ToString()
                                                                , row.ItemArray[1].ToString()
                                                                , "Y"
                                                                );
                        }
                        else
                        {
                            count += dwDataAccess.InsertGDHoanTraSMSToDW(
                                                               SMS_TYPE, message
                                                               , mobile //    ,long.Parse(row.ItemArray[10].ToString())
                                                               , DateTime.Parse(row.ItemArray[0].ToString())
                                //, row.ItemArray[4].ToString()
                                                               , row.ItemArray[16].ToString()
                                                               , row.ItemArray[2].ToString()
                                                               , row.ItemArray[3].ToString()
                                                               , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                               , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                               , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                               , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                               , row.ItemArray[9].ToString()
                                                               , row.ItemArray[11].ToString()
                                                               , row.ItemArray[12].ToString()
                                                               , row.ItemArray[14].ToString()
                                                               , row.ItemArray[1].ToString()
                                                               , "N"
                                                               );
                        }

                    }
                    else
                    {
                        count_err += dwDataAccess.InsertGDHoanTraSMSToDW(
                                                               SMS_TYPE, message
                                                               , mobile //    ,long.Parse(row.ItemArray[10].ToString())
                                                               , DateTime.Parse(row.ItemArray[0].ToString())
                            //, row.ItemArray[4].ToString()
                                                               , row.ItemArray[16].ToString()
                                                               , row.ItemArray[2].ToString()
                                                               , row.ItemArray[3].ToString()
                                                               , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                               , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                               , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                               , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                               , row.ItemArray[9].ToString()
                                                               , row.ItemArray[11].ToString()
                                                               , row.ItemArray[12].ToString()
                                                               , row.ItemArray[14].ToString()
                                                               , row.ItemArray[1].ToString()
                                                               , "E"
                                                               );
                    }
                }

            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            classGDHoanTraLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + count);
            classGDHoanTraLogWriter.WriteLog("Message loi khong Insert vao EbankGW: " + count_err);
            return;
        }
     

        private static string CreateSMSMessage(string brand, string pan, string date, string time,
                                       string amount, string crncyCode, string merchaneName, string vip_card, string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.PadLeft(8, '0'); // vi co truong hop chi co 7 ky tu
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                string crncyAlpha = classUtilities.ConvertCrncyCodeToCrncyAlpha(crncyCode);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string tmp_smsMessage = "SCB hoan tra " + amount1 + crncyAlpha + " cho the " + brand + " x" + pan
                + " giao dich tai ";
                string tmp_smsMessage2 = " luc " + time + " " + date + "\nDe biet them chi tiet LH " + SCBPhone;
                string smsMessage= tmp_smsMessage + merchaneName + tmp_smsMessage2;
                if (smsMessage.Length > 160)
                {
                    string tmp_merchaneName = merchaneName.Substring(0, 160 - tmp_smsMessage.Length - tmp_smsMessage2.Length -1);
                    smsMessage = tmp_smsMessage + tmp_merchaneName + tmp_smsMessage2;
                }
                return smsMessage;
            }
            catch (Exception ex)
            {
                classGDHoanTraLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }
        }

    }
}
