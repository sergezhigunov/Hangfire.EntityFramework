﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace Hangfire.EntityFramework {
    using System;
    using System.Reflection;
    
    
    /// <summary>
    ///    A strongly-typed resource class, for looking up localized strings, etc.
    /// </summary>
    // This class was auto-generated by the StronglyTypedResourceBuilder
    // class via a tool like ResGen or Visual Studio.
    // To add or remove a member, edit your .ResX file then rerun ResGen
    // with the /str option, or rebuild your VS project.
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    public class ErrorStrings {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        internal ErrorStrings() {
        }
        
        /// <summary>
        ///    Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        public static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("Hangfire.EntityFramework.ErrorStrings", typeof(ErrorStrings).GetTypeInfo().Assembly);
                    resourceMan = temp;
                }
                return resourceMan;
            }
        }
        
        /// <summary>
        ///    Overrides the current thread's CurrentUICulture property for all
        ///    resource lookups using this strongly typed resource class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        public static global::System.Globalization.CultureInfo Culture {
            get {
                return resourceCulture;
            }
            set {
                resourceCulture = value;
            }
        }
        
        /// <summary>
        ///    Looks up a localized string similar to Cannot retrieve provider instances registered for the folowing queues: {0}..
        /// </summary>
        public static string CannotRetrieveQueueProviderForQueue {
            get {
                return ResourceManager.GetString("CannotRetrieveQueueProviderForQueue", resourceCulture);
            }
        }
        
        /// <summary>
        ///    Looks up a localized string similar to Could not place a lock on the resource {0}. Lock timed out..
        /// </summary>
        public static string LockTimedOutOnResource {
            get {
                return ResourceManager.GetString("LockTimedOutOnResource", resourceCulture);
            }
        }
        
        /// <summary>
        ///    Looks up a localized string similar to Multiple provider instances registered for the folowing queues: {0}. You should choose only one type of persistent queues per server instance..
        /// </summary>
        public static string MultipleQueueProvidersNotSupported {
            get {
                return ResourceManager.GetString("MultipleQueueProvidersNotSupported", resourceCulture);
            }
        }
        
        /// <summary>
        ///    Looks up a localized string similar to Non-negative value required..
        /// </summary>
        public static string NeedNonNegativeValue {
            get {
                return ResourceManager.GetString("NeedNonNegativeValue", resourceCulture);
            }
        }
        
        /// <summary>
        ///    Looks up a localized string similar to Non-negative value required..
        /// </summary>
        public static string NeedPositiveValue {
            get {
                return ResourceManager.GetString("NeedPositiveValue", resourceCulture);
            }
        }
        
        /// <summary>
        ///    Looks up a localized string similar to Queues cannot be empty..
        /// </summary>
        public static string QueuesCannotBeEmpty {
            get {
                return ResourceManager.GetString("QueuesCannotBeEmpty", resourceCulture);
            }
        }
        
        /// <summary>
        ///    Looks up a localized string similar to String cannot be empty..
        /// </summary>
        public static string StringCannotBeEmpty {
            get {
                return ResourceManager.GetString("StringCannotBeEmpty", resourceCulture);
            }
        }
        
        /// <summary>
        ///    Looks up a localized string similar to The timeout specified is too large. Please supply a timeout less or equal to &apos;{0}&apos;..
        /// </summary>
        public static string TimeoutTooLarge {
            get {
                return ResourceManager.GetString("TimeoutTooLarge", resourceCulture);
            }
        }
    }
}
