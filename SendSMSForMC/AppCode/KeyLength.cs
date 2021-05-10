using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SendSMSForMC.AppCode
{
    //class KeyLength
    public enum  KeyLength
    {
        /**
       <summary>
       Single length keys.
       </summary>
       <remarks>
       Defines a single length hexadecimal key (16 digits).
       </remarks>
        * */
        SingleLength = 0,

        /**
        <summary>
        Double length keys.
        </summary>
        <remarks>
        Defines a double length hexadecimal key (32 digits).
        </remarks>
         * */
        DoubleLength,

        /**
        <summary>
        Triple length keys.
        </summary>
        <remarks>
        Defines a triple length hexadecimal key (48 digits).
        </remarks>
         * */
        TripleLength

    }
}
