using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SendSMSForMC.CardworksWS;
using System.ComponentModel;
using System.Data;
using System.Threading;
using System.ServiceModel;
using System.Security.Cryptography;


namespace SendSMSForMC.AppCode
{
    class classChange_PIN
    {
        static Random random = new Random();
        public static PINSelectionRespBean PINChangeViaWS(string pinEncrypted, string mobilePhone, string locPan)
        {
            PINSelectionRespBean resp = new PINSelectionRespBean();
            try
            {
            CardworksClient cw = new CardworksClient();            
            //string linkWS = classUtilities.GetStringValueFromConfig("linkWS_UAT");
            string linkWS = classUtilities.GetStringValueFromConfig("linkWS_LIVE");           
           
            cw.Endpoint.Address = new EndpointAddress(new Uri(linkWS));
            System.Net.ServicePointManager.ServerCertificateValidationCallback = delegate { return true; };

            PINSelectionReqBean req = new PINSelectionReqBean();
            req.sequenceNo = generateNumber();
            req.fi = "970429"; //Hard code
            req.pan = locPan; //LOC + 4 last digit Pan
            req.actInd = "3"; //Hard code
            req.mobileNo = mobilePhone;
            req.newPIN = pinEncrypted;

            //PINSelectionRespBean 
            resp = cw.PINSelection(req);
            return resp;
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error PINChangeViaWS() at change_pin class, " + ex.Message);
                return resp;
            }
        }

        public static string generateNumber()
        {
            try
            {
            string r = "";
            int i;
            for (i = 1; i < 11; i++)
            {
                r += random.Next(0, 9).ToString();
            }
            return r;
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error generateNumber() at change_pin class, " + ex.Message);
                return "";
            }
        }
        public int Change_PIN(string mobile_i, string pin_i, string zpk_i,string locPan_i)
        {
            try
            {
                string mobile = "";
                string pin = pin_i;
                string zpk = zpk_i;
                string locPan = locPan_i;
                string ResSequenceNo = "";
                string ResResponseCode = "";
                string ResponseDescription = "";

                mobile = mobile_i.Substring(mobile_i.Length - 8, 8);
                string clearPINClock = "16" + pin + mobile;
                string encryptedPIN = TripleDESEncrypt(new HexKey(zpk), clearPINClock);
                encryptedPIN = encryptedPIN.ToUpper();

                PINSelectionRespBean pinChangeResp = PINChangeViaWS(encryptedPIN, mobile, locPan);
                if (pinChangeResp != null)
                {
                    ResSequenceNo = pinChangeResp.sequenceNo;
                    ResResponseCode = pinChangeResp.responseCode;
                    ResponseDescription = pinChangeResp.responseDescription;
                }
                if (ResResponseCode == "000")//success
                    return 1;
                else
                    return 0;//fail
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error Change_PIN() at change_pin class, " + ex.Message);              
                return 0;
            }
        }
        public static string TripleDESEncrypt(HexKey key, string data)
        {
            try
            {
            string result = "";
            if (data.Length == 16)
                result = Encrypt(key, data);
            return result;
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error TripleDESEncrypt() at change_pin class, " + ex.Message);
                return "";
            }
        }
        private static string Encrypt(HexKey key, string data)
        {
            try
            {
            string result = "";
            result = DES.DESEncrypt(key.getPartA(), data);
            result = DES.DESDecrypt(key.getPartB(), result);
            result = DES.DESEncrypt(key.getPartC(), result);
            return result;
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error Encrypt() at change_pin class, " + ex.Message);
                return "";
            }

        }
    }
}
