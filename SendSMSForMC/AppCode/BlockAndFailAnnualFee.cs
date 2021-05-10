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
    class classBlockAndFailAnnualFee
    {
        public static bool _exitThread = false;
        public static string _updateDateTime = null;

        private static classDataAccess _dataAccess = new classDataAccess();

        public static string SMS_TYPE = "BAFAF";

        private static string SCBPhone = "";

        public static void RunService()
        {

            int minute = 0;
            int hour = 0;

            DataTable table = new DataTable();
            SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");           

            int hourValue = 0;
            int minuteValue = 0;
            hourValue = classUtilities.GetIntValueFromConfig("Accumulative_BlockAndFailFee_Hour");
            minuteValue = classUtilities.GetIntValueFromConfig("Accumulative_BlockAndFailFee_Minute");


            while (_exitThread == false)
            {
                try
                {
                    hour = DateTime.Now.Hour;
                    minute = DateTime.Now.Minute;
                   
                    if (hour == hourValue && minute == minuteValue)
                    //if (1==1) //hhhh
                    {

                       

                            classBlockAndFailAnnualFeeLogWriter.WriteLog("----------------Begin Process-----------------");
                            _dataAccess = new classDataAccess();
                            table.Rows.Clear();
                            table = GetBlockAndFailAnnualFee();
                            if (table.Rows.Count > 0)
                                Insert_SMSMessage(table);
                                //Insert_SMSMessage_S(table);

                            classBlockAndFailAnnualFeeLogWriter.WriteLog("----------------End Process-----------------");
                            //Thread.Sleep(1000 * 3600 * 23);// sleep 23h sau khi thuc hien xong
                            Thread.Sleep(1000 * 60);// de khong lap lap lai vong lap lan 2 trong ngay
                       
                    }
                    else
                    {
                        if ((hour == hourValue && minute > minuteValue) || (hour > hourValue))// da qua gio thuc thi, sleep de chuyen den ngay tiep theo
                        {
                            classBlockAndFailAnnualFeeLogWriter.WriteLog("((hour == hourValue && minute > minuteValue) || (hour > hourValue)) sleep(minute): " + (60 * (24 - (hour - hourValue + 1))));
                            Thread.Sleep(1000 * (60 * 60) * (24 - (hour - hourValue + 1)));

                        }
                        else
                        {
                            if (hour < (hourValue - 1))
                            {
                                classBlockAndFailAnnualFeeLogWriter.WriteLog("(hour < hourValue) sleep(miute): " + ((hourValue - hour - 1) * 60 - minute));
                                Thread.Sleep(1000 * 60 * ((hourValue - hour - 1) * 60 - minute + minuteValue - 1));
                            }
                            else
                            {
                                if (hour == (hourValue - 1))
                                {
                                    classBlockAndFailAnnualFeeLogWriter.WriteLog("(hour == (hourValue - 1)) sleep(minute): " + (60 - minute + minuteValue - 1));
                                    Thread.Sleep((1000 * 60) * (60 - minute + minuteValue - 1));
                                    if ((60 - minute + minuteValue - 1) == 0)//1 phut truoc gio gui tn
                                        Thread.Sleep(1000 * 10);
                                }
                                else//(hour==hourValue)
                                {
                                    if (minute < (minuteValue - 1))
                                    {
                                        classBlockAndFailAnnualFeeLogWriter.WriteLog("(minute <= (minuteValue - 1)) sleep(minute): " + (minuteValue - minute - 1));
                                        Thread.Sleep((1000 * 60) * (minuteValue - minute - 1));
                                    }
                                    else
                                    {
                                        classBlockAndFailAnnualFeeLogWriter.WriteLog("sleep(ss): " + 10);
                                        Thread.Sleep(1000 * 10);
                                    }
                                }
                            }
                        }
                    }
                    //Thread.Sleep(1000 * 50); // 1 phut moi check 1 lan
                }
                catch (Exception ex)
                {
                    classBlockAndFailAnnualFeeLogWriter.WriteLog("Error RunService(), " + ex.Message);
                }
            }
        }

        private static DataTable GetBlockAndFailAnnualFee()
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
                    classBlockAndFailAnnualFeeLogWriter.WriteLog("Error GetBlockAndFailAnnualFee(), " + ex.Message);
                    return null;
                }
                if (maxUpdateDT != null)
                    table = _dataAccess.GetBlockAndFailAnnualFee(maxUpdateDT);
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
            int resultDW = 0;
            int resultGW = 0;
            int countDW = 0;
            int count_errDW = 0;
            int countGW = 0;
            int count_errGW = 0;

            //OracleCommand cmd = ebankDataAccess.AddProcedureParameterToEBankGW();           l

            foreach (DataRow row in table.Rows)
            {
                resultDW = 0;
                resultGW = 0;
                message = "";//reset
                if (row.ItemArray[11].ToString() == "2" && row.ItemArray[12].ToString() == "N")
                {
                    message = CreateSMSMessageFailAnnualFee(row.ItemArray[5].ToString(), row.ItemArray[1].ToString(), row.ItemArray[3].ToString(),
                                                        row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[16].ToString(), row.ItemArray[17].ToString());
                }

                if (row.ItemArray[11].ToString() == "7" && row.ItemArray[12].ToString() == "F" && row.ItemArray[12].ToString() != "" )
                {
                    message = CreateSMSMessageBlockCard(row.ItemArray[5].ToString(), row.ItemArray[1].ToString(), row.ItemArray[3].ToString(),
                                                        row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[16].ToString(), row.ItemArray[17].ToString());
                }
               
                if (string.IsNullOrEmpty(message) == false)
                {
                    //string mobile = classUtilities.GetMobileFromCardNoOfSpecialList(row.ItemArray[10].ToString(), row.ItemArray[7].ToString());
                    string mobile = row.ItemArray[7].ToString();

                    if (row.ItemArray[7].ToString() == "khong co")
                    {
                        resultDW = dwDataAccess.InsertBlockAndFailAnnualFeeSMSToDW(
                                                            SMS_TYPE
                                                            , message
                                                            , mobile//    ,long.Parse(row.ItemArray[10].ToString())
                                                            , DateTime.Parse(row.ItemArray[0].ToString())
                            //, row.ItemArray[4].ToString()
                                                            , row.ItemArray[10].ToString()
                                                            , row.ItemArray[5].ToString()
                                                            , row.ItemArray[6].ToString()
                                                            , row.ItemArray[3].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                            , row.ItemArray[2].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                            , row.ItemArray[4].ToString()//    , long.Parse(row.ItemArray[7].ToString())
                                                            , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())                                                               
                                                            , "Y"
                                                            );
                    }
                    else
                    {
                        resultDW = dwDataAccess.InsertBlockAndFailAnnualFeeSMSToDW(
                                                            SMS_TYPE
                                                            , message
                                                            , mobile//    ,long.Parse(row.ItemArray[10].ToString())
                                                            , DateTime.Parse(row.ItemArray[0].ToString())
                            //, row.ItemArray[4].ToString()
                                                            , row.ItemArray[10].ToString()
                                                            , row.ItemArray[5].ToString()
                                                            , row.ItemArray[6].ToString()
                                                            , row.ItemArray[3].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                            , row.ItemArray[2].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                            , row.ItemArray[4].ToString()//    , long.Parse(row.ItemArray[7].ToString())
                                                            , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())                                                               
                                                            , "N"
                                                            );
                        if (resultDW == 1)
                        {
                            resultGW = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                               row.ItemArray[9].ToString()
                                                                               , mobile
                                                                               , message
                                                                               , 'N'//Y se ko gui tin nhan, N se gui tin nhan
                                                                               , SMS_TYPE);
                            if (resultGW == 0)
                            {
                                dwDataAccess.Update_Status_SMS_All(row.ItemArray[10].ToString(), row.ItemArray[8].ToString(), SMS_TYPE, "E");
                                count_errGW++;
                                classBlockAndFailAnnualFeeLogWriter.WriteLog("err: card no " + row.ItemArray[10].ToString() + " can't insert DB EB");
                            }
                            else
                            {

                                countGW++;
                            }
                            countDW++;

                        }
                        else//intert DW khong thanh cong
                        {
                            count_errDW++;
                            classBlockAndFailAnnualFeeLogWriter.WriteLog("err: card no " + row.ItemArray[10].ToString() + " can't insert DB DW");
                            
                        }

                       
                    }
                  
                   
                    

                }

            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            classBlockAndFailAnnualFeeLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + countGW);
            classBlockAndFailAnnualFeeLogWriter.WriteLog("Message loi khong Insert vao EbankGW: " + count_errGW);
            classBlockAndFailAnnualFeeLogWriter.WriteLog("Message da duoc Insert vao DW thanh cong: " + countDW);
            classBlockAndFailAnnualFeeLogWriter.WriteLog("Message loi khong Insert vao DW: " + count_errDW);
            return;
        }

        private static void Insert_SMSMessage_S(DataTable table)
        {
            classDataAccess ebankDataAccess = new classDataAccess();
            classDataAccess dwDataAccess = new classDataAccess();
            ebankDataAccess.OpenConnection("EBANK_GW");
            dwDataAccess.OpenConnection("CW_DW");

            string message = "";
            int resultDW = 0;
            int resultGW = 0;
            int countDW = 0;
            int count_errDW = 0;
            int countGW = 0;
            int count_errGW = 0;

            //OracleCommand cmd = ebankDataAccess.AddProcedureParameterToEBankGW();           l

            foreach (DataRow row in table.Rows)
            {
                resultDW = 0;
                resultGW = 0;
                message = "";//reset
                //message = CreateSMSMessageS(row.ItemArray[5].ToString(), row.ItemArray[1].ToString(), row.ItemArray[3].ToString(),
                //                                        row.ItemArray[2].ToString(), row.ItemArray[4].ToString());
                message = CreateSMSMessageFailAnnualFee(row.ItemArray[5].ToString(), row.ItemArray[1].ToString(), row.ItemArray[3].ToString(),
                                                       row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[16].ToString(), row.ItemArray[17].ToString());
                
               
                if (string.IsNullOrEmpty(message) == false)
                {
                    //string mobile = classUtilities.GetMobileFromCardNoOfSpecialList(row.ItemArray[10].ToString(), row.ItemArray[7].ToString());
                    string mobile = row.ItemArray[7].ToString();

                    if (row.ItemArray[10].ToString() == "khong co")
                    {
                        resultDW = dwDataAccess.InsertBlockAndFailAnnualFeeSMSToDW(
                                                            SMS_TYPE
                                                            , message
                                                            , mobile//    ,long.Parse(row.ItemArray[10].ToString())
                                                            , DateTime.Parse(row.ItemArray[0].ToString())
                            //, row.ItemArray[4].ToString()
                                                            , row.ItemArray[10].ToString()
                                                            , row.ItemArray[5].ToString()
                                                            , row.ItemArray[6].ToString()
                                                            , row.ItemArray[3].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                            , row.ItemArray[2].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                            , row.ItemArray[4].ToString()//    , long.Parse(row.ItemArray[7].ToString())
                                                            , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())                                                               
                                                            , "Y"
                                                            );
                    }
                    else
                    {
                        resultDW = dwDataAccess.InsertBlockAndFailAnnualFeeSMSToDW(
                                                            SMS_TYPE
                                                            , message
                                                            , mobile//    ,long.Parse(row.ItemArray[10].ToString())
                                                            , DateTime.Parse(row.ItemArray[0].ToString())
                            //, row.ItemArray[4].ToString()
                                                            , row.ItemArray[10].ToString()
                                                            , row.ItemArray[5].ToString()
                                                            , row.ItemArray[6].ToString()
                                                            , row.ItemArray[3].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                            , row.ItemArray[2].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                            , row.ItemArray[4].ToString()//    , long.Parse(row.ItemArray[7].ToString())
                                                            , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())    
                                                            , "N"
                                                            );
                    }
                    if (resultDW == 1)
                    {
                        countDW++;
                        if (row.ItemArray[7].ToString() == "khong co")
                        {

                            resultGW = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                               row.ItemArray[9].ToString()
                                                                               , mobile
                                                                               , message
                                                                               , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
                                                                               , SMS_TYPE);
                        }
                        else
                        {
                            resultGW = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                                row.ItemArray[9].ToString()
                                                                                , mobile
                                                                                , message
                                                                                , 'N'//Y se ko gui tin nhan, N se gui tin nhan
                                                                                , SMS_TYPE);
                        }
                        if (resultGW == 1)
                            countGW++;
                        else
                            count_errGW++;
                    }
                    else
                    {
                        count_errDW++;
                    }


                }

            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            classBlockAndFailAnnualFeeLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + countGW);
            classBlockAndFailAnnualFeeLogWriter.WriteLog("Message loi khong Insert vao EbankGW: " + count_errGW);
            classBlockAndFailAnnualFeeLogWriter.WriteLog("Message da duoc Insert vao DW thanh cong: " + countDW);
            classBlockAndFailAnnualFeeLogWriter.WriteLog("Message loi khong Insert vao DW: " + count_errDW);
            return;
        }

        private static string CreateSMSMessageS(string brand, string pan, string date, string time,
                                      string amount)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);


                string smsMessage = "Xin loi Quy Khach vi su bat tien nay. The " + brand + " Debit x" + pan
                + " cua Quy khach dang hoat dong binh thuong. Chi tiet LH 1800545438";


                return smsMessage;
            }
            catch (Exception ex)
            {
                classBlockAndFailAnnualFeeLogWriter.WriteLog("Error CreateSMSMessageS(), " + ex.Message);
                return "";
            }
        }
        private static string CreateSMSMessageFailAnnualFee(string brand, string pan, string date, string time,
                                       string amount, string vip_card,string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                if (vip_card == "Y" || vip_cif=="Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string smsMessage = "Tai Khoan Quy Khach khong du de thanh toan phi The SCB " + brand + " Debit x" + pan
                + ". Quy khach vui long nop/chuyen tien vao Tai khoan. Chi tiet LH " + SCBPhone;
                

                return smsMessage;
            }
            catch (Exception ex)
            {
                classBlockAndFailAnnualFeeLogWriter.WriteLog("Error CreateSMSMessageFailAnnualFee(), " + ex.Message);
                return "";
            }
        }


        private static string CreateSMSMessageBlockCard(string brand, string pan, string date, string time,
                                       string amount, string vip_card, string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                if (vip_card == "Y" || vip_cif =="Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string smsMessage = "The SCB " + brand + " Debit x" + pan
                + " cua Quy khach bi tam khoa do khong thu duoc phi tu tai khoan. Vui long lien he SCB de nop phi va mo khoa the. LH " + SCBPhone;
                
                return smsMessage;
            }
            catch (Exception ex)
            {
                classBlockAndFailAnnualFeeLogWriter.WriteLog("Error CreateSMSMessageBlockCard(), " + ex.Message);
                return "";
            }
        }
    }
}
