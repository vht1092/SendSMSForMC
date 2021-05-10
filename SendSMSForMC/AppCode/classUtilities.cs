using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Forms;
using System.IO;

namespace SendSMSForMC.AppCode
{
    public class classUtilities
    {
        public static string[] MYPHONE = new string[] { "0912835638", "0906094098", "0983986298", "0906806988" };
        public static List<string[]> _currencyMapping = new List<string[]>();
        private static List<string[]> _specialCardList = new List<string[]>();
        private static List<string[]> _PinSMSCardList = new List<string[]>();
        private static List<string[]> _PilotCardList = new List<string[]>();
        public static List<string[]> _2PhoneCardList = new List<string[]>();
        private static string DEFAULT_CRNCY_ALPA = "MTT";

        public static void GetMappingFile()
        {
            _currencyMapping = classUtilities.ReadMappingFile("currency_mapping.txt");
            _specialCardList = classUtilities.ReadMappingFile("special_card_list.txt");
            _PinSMSCardList = classUtilities.ReadMappingFile("pin_sms_card_list.txt");
            _PilotCardList = classUtilities.ReadMappingFile("pilot_card_list.txt");
            _2PhoneCardList = classUtilities.ReadMappingFile("2phone_card_list.txt");
        }

        public static string GetRandomMobile()
        {
            string mobile = "";
            Random rd = new Random(0);
            mobile = MYPHONE[rd.Next(0, MYPHONE.Length)].ToString();
            return mobile;
        }

        public static int GetIntValueFromConfig(string keyName)
        {
            int value = int.MaxValue;
            try
            {
                string keyValue = System.Configuration.ConfigurationManager.AppSettings[keyName];
                value = int.Parse(keyValue);
            }
            catch (Exception ex)
            {
                classDataAccessLogWriter.WriteLog("-------------Error: GetValueFromConfig() with " + keyName);
                classDataAccessLogWriter.WriteLog(ex.Message);
            }
            return value;
        }
        public static string GetStringValueFromConfig(string keyName)
        {
            string keyValue = "";
            try
            {
                keyValue = System.Configuration.ConfigurationManager.AppSettings[keyName];
                return keyValue;
            }
            catch (Exception ex)
            {
                classDataAccessLogWriter.WriteLog("-------------Error: GetValueFromConfig() with " + keyName);
                classDataAccessLogWriter.WriteLog(ex.Message);
            }
            return keyValue;
        }

        public static List<string[]> ReadMappingFile(string filename)
        {
            try
            {
                string rootpath = Application.StartupPath;
                filename = rootpath + "\\" + filename;
                FileStream fs = new FileStream(filename, FileMode.Open);
                StreamReader r = new StreamReader(fs, Encoding.ASCII);
                List<string[]> data = new List<string[]>();
                while (r.EndOfStream == false)
                {
                    data.Add(r.ReadLine().Split('|'));
                }
                r.Close();
                fs.Close();
                return data;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error ReadCurrencyMappingFile(), " + ex.Message);
            }
            return null;
        }

        public static string GetCardFromSMS_Pin_List(string cardNo)
        {
            try
            {
                foreach (string[] item in classUtilities._PinSMSCardList)
                {
                    if (item[0] == cardNo)
                        return item[0];
                }
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error GetCardFromSMS_Pin_List(), " + ex.Message);
            }
            return "";
        }

        public static string GetMobileFromCardNoOfSpecialList(string cardNo, string mobile)
        {
            try
            {
                foreach (string[] item in classUtilities._specialCardList)
                {
                    if (item[0] == cardNo)
                        return item[1];
                }
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error GetMobileFromCardNoOfSpecialList(), " + ex.Message);
            }
            return mobile;
        }
        public static string GetMobileFromCardNoOfSpecialList2(string cardNo)
        {
            try
            {
                foreach (string[] item in classUtilities._specialCardList)
                {
                    if (item[0] == cardNo)
                        return item[1];
                }
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error GetMobileFromCardNoOfSpecialList2(), " + ex.Message);
            }
            return "000";
        }
        public static string GetMobileFromCardNoOfPilotList(string cardNo)
        {
            try
            {
                foreach (string[] item in classUtilities._PilotCardList)
                {
                    if (item[0] == cardNo)
                        return item[0];
                }
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error GetMobileFromCardNoOfPilotList(), " + ex.Message);
            }
            return "";
        }
       
        public static string GetMobileFromCardNoOf2PhoneList(string cardNo)
        {
            try
            {
                foreach (string[] item in classUtilities._2PhoneCardList)
                {
                    
                    if (item[0] == cardNo)                     
                        return item[2];//tra ve so dt thu 2
                }
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error GetMobileFromCardNoOf2PhoneList(), " + ex.Message);
            }
            return "";
        }
        public static string ConvertCrncyCodeToCrncyAlpha(string code)
        {
            try
            {

                foreach (string[] item in classUtilities._currencyMapping)
                {
                    if (item[0] == code)
                        return item[1];
                }
                return DEFAULT_CRNCY_ALPA;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error ConvertCrncyCodeToCrncyAlpha(), " + ex.Message);
                return DEFAULT_CRNCY_ALPA;
            }
            
        }
    }
}
