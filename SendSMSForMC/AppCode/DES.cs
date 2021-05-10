using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Security.Cryptography;

namespace SendSMSForMC.AppCode
{
    class DES
    {
        /**
       <summary>
       Encrypts a hex string.
       </summary>
       <remarks>
       This method encrypts hex data under a hex key and returns the result.
       </remarks>
        * */
        public static string DESEncrypt(string sKey, string sData)
        {
            //Dim bOutput() As Byte = {}, sResult As String = ""
            byte[] bOutput = byteDESEncrypt(Utility.HexToByteArray(sKey), Utility.HexToByteArray(sData));
            return Utility.ByteArrayToHex(bOutput);

        }

        /**
        <summary>
        Encrypts a byte array.
        </summary>
        <remarks>
        The method encrypts a byte array of 16 bytes.
        </remarks>
         * */
        public static byte[] byteDESEncrypt(byte[] bKey, byte[] bData)
        {
            byte[] bResult = new byte[8];

            //Using outStream As MemoryStream = New MemoryStream(bResult);
            MemoryStream outStream = new MemoryStream(bResult);
            //Using desProvider As DESCryptoServiceProvider = New DESCryptoServiceProvider
            DESCryptoServiceProvider desProvider = new DESCryptoServiceProvider();
            byte[] bNullVector = { 0, 0, 0, 0, 0, 0, 0, 0 };

            desProvider.Mode = CipherMode.ECB;
            desProvider.Key = bKey;
            desProvider.IV = bNullVector;
            desProvider.Padding = PaddingMode.None;

            //Using cStream As CryptoStream = New CryptoStream(outStream, desProvider.CreateEncryptor(bKey, bNullVector), CryptoStreamMode.Write)
            CryptoStream cStream = new CryptoStream(outStream, desProvider.CreateEncryptor(bKey, bNullVector), CryptoStreamMode.Write);
            cStream.Write(bData, 0, 8);
            cStream.Close();

            return bResult;

        }

        /**
        DES-decrypt a 16-hex block using a 16-hex key
        <summary>
        Decrypts a hex string.
        </summary>
        <remarks>
        This method decrypts hex data using a hex key and returns the result.
        </remarks>
         * */
        public static string DESDecrypt(string sKey, string sData)
        {
            byte[] bOutput = byteDESDecrypt(Utility.HexToByteArray(sKey), Utility.HexToByteArray(sData));
            return Utility.ByteArrayToHex(bOutput);
        }

        /**
        <summary>
        Decrypts a byte array.
        </summary>
        <remarks>
        This method decrypts a byte array of 16 bytes.
        </remarks>
         * */
        public static byte[] byteDESDecrypt(byte[] bKey, byte[] bData)
        {
            byte[] bResult = new byte[8];
            DESCryptoServiceProvider desProvider = new DESCryptoServiceProvider();
            byte[] bNullVector = { 0, 0, 0, 0, 0, 0, 0, 0 };

            desProvider.Mode = CipherMode.ECB;
            desProvider.Key = bKey;
            desProvider.IV = bNullVector;
            desProvider.Padding = PaddingMode.None;

            MemoryStream outStream = new MemoryStream(bResult);
            CryptoStream cStream = new CryptoStream(outStream, desProvider.CreateDecryptor(bKey, bNullVector), CryptoStreamMode.Write);
            cStream.Write(bData, 0, 8);
            cStream.Close();

            return bResult;
        }
    }
}
