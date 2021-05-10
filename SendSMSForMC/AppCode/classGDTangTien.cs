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
    class classGDTangTien
    {
        public static bool _exitThread = false;
        public static string _updateDateTime = null;

        private static classDataAccess _dataAccess = new classDataAccess();

        public static string SMS_TYPE = "GDTT";

        private static string SCBPhone = "";

        public static void RunService()
        {

            int minute = 0;
            int hour = 0;
            DataTable table = new DataTable();
            SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            int minuteValue = classUtilities.GetIntValueFromConfig("GDTangTien_Minute");
            int hourValue = classUtilities.GetIntValueFromConfig("GDTangTien_Hour");
          
            while (_exitThread == false)
            {
                try
                {
                    minute = DateTime.Now.Minute;
                    hour = DateTime.Now.Hour;
                    if (hour == hourValue && minute == minuteValue)             
                    //if(1==1)//hhhh
                    {
                       
                        classGDTangTienLogWriter.WriteLog("----------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();
                        table.Rows.Clear(); 
                        table = Get_GDTangTien();
                        if (table.Rows.Count > 0)
                            Insert_SMSMessage(table);

                        //if (System.DateTime.Today.ToString("dd") == "16" || System.DateTime.Today.ToString("dd") == "26")
                        ////if(1==1)
                        //{
                        //    _dataAccess = new classDataAccess();
                        //    table.Rows.Clear();
                        //    string month = System.DateTime.Today.ToString("yyyyMM");
                        //    string Card_Brn = null;
                        //    string temp = System.DateTime.Today.ToString("dd");
                        //    if (System.DateTime.Today.ToString("dd") == "16")                    
                        //        Card_Brn = "VS";
                        //    if (System.DateTime.Today.ToString("dd") == "26")  
                        //        Card_Brn = "MC";

                        //    table = Get_GDTangTien_New(month, Card_Brn);
                        //    if (table.Rows.Count > 0)
                        //        Insert_SMSMessage(table);
                        //}
                        classGDTangTienLogWriter.WriteLog("----------------End Process-----------------");
                        Thread.Sleep(1000 * 60);
                    }
                    else
                    {

                        if ((hour == hourValue && minute > minuteValue) || (hour > hourValue))// da qua gio thuc thi, sleep de chuyen den ngay tiep theo
                        {
                            classGDTangTienLogWriter.WriteLog("((hour == hourValue && minute > minuteValue) || (hour > hourValue)) sleep(minute): " + (60 * (24 - (hour - hourValue + 1))));
                            Thread.Sleep(1000 * (60 * 60) * (24 - (hour - hourValue + 1)));

                        }
                        else
                        {
                            if (hour < (hourValue - 1))
                            {
                                classGDTangTienLogWriter.WriteLog("(hour < hourValue) sleep(miute): " + ((hourValue - hour - 1) * 60 - minute + minuteValue - 1));
                                Thread.Sleep(1000 * 60 * ((hourValue - hour - 1) * 60 - minute + minuteValue - 1));
                            }
                            else
                            {
                                if (hour == (hourValue - 1))
                                {
                                    classGDTangTienLogWriter.WriteLog("(hour == (hourValue - 1)) sleep(minute): " + (60 - minute + minuteValue - 1));
                                    Thread.Sleep((1000 * 60) * (60 - minute + minuteValue - 1));
                                    if ((60 - minute + minuteValue - 1) == 0)//1 phut truoc gio gui tn
                                        Thread.Sleep(1000 * 10);
                                }
                                else//(hour==hourValue)
                                {
                                    if (minute < (minuteValue - 1))
                                    {
                                        classGDTangTienLogWriter.WriteLog("(minute <= (minuteValue - 1)) sleep(minute): " + (minuteValue - minute - 1));
                                        Thread.Sleep((1000 * 60) * (minuteValue - minute - 1));
                                    }
                                    else
                                    {
                                        classGDTangTienLogWriter.WriteLog("sleep(ss): " + 10);
                                        Thread.Sleep(1000 * 10);
                                    }
                                }
                            }
                        }
                    }
                   
                }
                catch (Exception ex)
                {
                    classGDTangTienLogWriter.WriteLog("Error RunService(), " + ex.Message);
                }
            }
        }

        private static DataTable Get_GDTangTien_New(string month, string Card_Brn)
        {
            try
            {               
                DataTable table = new DataTable();
                table = _dataAccess.GetGDTangTien_New(month, Card_Brn);
                return table;
                   
            }
            catch (Exception ex)
            {
                classGDTangTienLogWriter.WriteLog("Error Get_GDTangTien_New(), " + ex.Message);
                return null;
            }        
            
        }

        private static DataTable Get_GDTangTien()
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
                    classGDTangTienLogWriter.WriteLog("Error Get_GDTangTien(), " + ex.Message);
                    return null;
                }
                if (maxUpdateDT != null)
                    table = _dataAccess.GetGDTangTien(maxUpdateDT);
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
                                                        row.ItemArray[8].ToString(), row.ItemArray[16].ToString(), row.ItemArray[17].ToString());
                /////////
                //string month = System.DateTime.Today.ToString("yyyyMM");
                //message = CreateSMSMessage_new(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(),
                //                                        row.ItemArray[6].ToString(), row.ItemArray[7].ToString(),
                //                                        row.ItemArray[8].ToString(), row.ItemArray[16].ToString(), row.ItemArray[17].ToString(), month);
                if (string.IsNullOrEmpty(message) == false)
                {
                    //string mobile = classUtilities.GetMobileFromCardNoOfSpecialList(row.ItemArray[4].ToString(), row.ItemArray[10].ToString());
                    string mobile = row.ItemArray[10].ToString();
                    if (row.ItemArray[10].ToString() == "khong co")
                    {
                        result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                           row.ItemArray[14].ToString()
                                                                           , mobile
                                                                           , message
                                                                           , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
                                                                           , SMS_TYPE);
                    }
                    else
                    {
                        result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                            row.ItemArray[14].ToString()
                                                                            , mobile
                                                                            , message
                                                                            , 'N'//Y se ko gui tin nhan, N se gui tin nhan
                                                                            , SMS_TYPE);
                    }
                    if (result == 1)
                    {

                        if (row.ItemArray[10].ToString() == "khong co")
                        {
                            count += dwDataAccess.InsertGDTangTienSMSToDW(
                                                                SMS_TYPE
                                                                , message
                                                                , mobile//    ,long.Parse(row.ItemArray[10].ToString())
                                                                , DateTime.Parse(row.ItemArray[0].ToString())
                                //, row.ItemArray[4].ToString()
                                                                , row.ItemArray[15].ToString()
                                                                , row.ItemArray[2].ToString()
                                                                , row.ItemArray[3].ToString()
                                                                , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                                , row.ItemArray[9].ToString()
                                                                , row.ItemArray[11].ToString()
                                                                , row.ItemArray[12].ToString()
                                                                , row.ItemArray[1].ToString()
                                                                , "Y"
                                                                );
                        }
                        else
                        {
                            count += dwDataAccess.InsertGDTangTienSMSToDW(
                                                                SMS_TYPE
                                                                , message
                                                                , mobile//    ,long.Parse(row.ItemArray[10].ToString())
                                                                , DateTime.Parse(row.ItemArray[0].ToString())
                                //, row.ItemArray[4].ToString()
                                                                , row.ItemArray[15].ToString()
                                                                , row.ItemArray[2].ToString()
                                                                , row.ItemArray[3].ToString()
                                                                , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                                , row.ItemArray[9].ToString()
                                                                , row.ItemArray[11].ToString()
                                                                , row.ItemArray[12].ToString()
                                                                , row.ItemArray[1].ToString()
                                                                , "N"
                                                                );
                        }
                    }
                    else
                    {
                        count_err += dwDataAccess.InsertGDTangTienSMSToDW(
                                                               SMS_TYPE
                                                               , message
                                                               , mobile//    ,long.Parse(row.ItemArray[10].ToString())
                                                               , DateTime.Parse(row.ItemArray[0].ToString())
                            //, row.ItemArray[4].ToString()
                                                               , row.ItemArray[15].ToString()
                                                               , row.ItemArray[2].ToString()
                                                               , row.ItemArray[3].ToString()
                                                               , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                               , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                               , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                               , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                               , row.ItemArray[9].ToString()
                                                               , row.ItemArray[11].ToString()
                                                               , row.ItemArray[12].ToString()
                                                               , row.ItemArray[1].ToString()
                                                               , "E"
                                                               );
                    }
                }

            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            classGDTangTienLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + count);
            classGDTangTienLogWriter.WriteLog("Message loi khong Insert vao EbankGW: " + count_err);
            return;
        }

        private static string CreateSMSMessage_new(string brand, string pan, string date, string time,
        string amount, string crncyCode, string vip_card, string vip_cif, string month)
        {
            try
            {
                pan = pan.Substring(12, 4); 
                string crncyAlpha = classUtilities.ConvertCrncyCodeToCrncyAlpha(crncyCode);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string temp_month = month.Substring(4) + "/" + month.Substring(0, 4);
                string smsMessage = "Cam on Quy khach da su dung the SCB " + pan + "\nQuy khach duoc tang "
                + amount1 + crncyAlpha + " theo chuong trinh uu dai cua SCB\nChi tiet tai sao ke thang " + temp_month
                + "\nLH "+SCBPhone;
                return smsMessage;
            }
            catch (Exception ex)
            {
                classGDTangTienLogWriter.WriteLog("Error CreateSMSMessage_new(), " + ex.Message);
                return "";
            }
        }
        private static string CreateSMSMessage(string brand, string pan, string date, string time,
                                       string amount, string crncyCode, string vip_card, string vip_cif)
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
                if (vip_card == "Y"||vip_cif=="Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string smsMessage = "Cam on Quy khach da su dung the SCB " + pan + "\nQuy khach duoc tang "
               + amount1 + crncyAlpha + " theo chuong trinh uu dai cua SCB\nLien he " + SCBPhone;
                return smsMessage;
            }
            catch (Exception ex)
            {
                classGDTangTienLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }
        }
    }
}
