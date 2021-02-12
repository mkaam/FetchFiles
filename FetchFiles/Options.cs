using CommandLine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FetchFiles
{
    class Options
    {
        public virtual bool Verbose { get; set; }

        public virtual string CsvFile { get; set; }

        public virtual string LogFile { get; set; }

        public virtual string ServerName { get; set; }

        public virtual string DBName { get; set; }

        public virtual string TableName { get; set; }

        public virtual string PathList { get; set; }
        
        public virtual IEnumerable<string> DirectoryFilters { get; set; }

        public virtual IEnumerable<string> FileFilters { get; set; }

        public virtual bool TruncateTable { get; set; }

    }

    [Verb("ExportToDB", isDefault: true, HelpText = "Genearte file list by criteria and save to Database")]
    class ExportToDB : Options
    {
        [Option(Required = true, HelpText = "Define pathlist from text file. Eg: C:\\PathList.txt")]
        public override string PathList { get; set; }

        [Option(Default = "*", Separator = ',', HelpText = "Input filter for directory to be processed. eg: 2020-12-24, 2020-12-21")]
        public override IEnumerable<string> DirectoryFilters { get; set; }

        [Option(Default = "*", Separator = ',', HelpText = "Input filter for files to be processed. eg: 2020-12-24, 2020-12-21")]
        public override IEnumerable<string> FileFilters { get; set; }

        [Option(Required = true, HelpText = "Server name for export list")]
        public override string ServerName { get; set; }
        
        [Option(Required = true, HelpText = "Database name for export list")]
        public override string DBName { get; set; }

        [Option(Required = true, HelpText = "Table name for export list")]
        public override string TableName { get; set; }

        [Option(HelpText = "Logging to console")]
        public override bool Verbose { get; set; }

        [Option(HelpText = "Logging to file: default is [ExePath]\\Logs\\yyyyMMdd.csv")]
        public override string LogFile { get; set; }

        [Option(Default = false, HelpText = "Truncate destination table before insert")]
        public override bool TruncateTable { get; set; }
    }

    [Verb("ExportToCSV", HelpText = "Genearte file list by criteria and save to CSV File")]
    class ExportToCSV : Options
    {
        [Option(Required = true, HelpText = "Define pathlist from text file. Eg: C:\\PathList.txt")]
        public override string PathList { get; set; }

        [Option(Default = "*", Separator = ',', HelpText = "Input filter for directory to be processed. eg: 2020-12-24, 2020-12-21")]
        public override IEnumerable<string> DirectoryFilters { get; set; }

        [Option(Default = "*", Separator = ',', HelpText = "Input filter for files to be processed. eg: 2020-12-24, 2020-12-21")]
        public override IEnumerable<string> FileFilters { get; set; }

        [Option(Required = true, HelpText = "FullPath CSV File Name for export File List. Eg: C:\\FileList.csv")]
        public override string CsvFile { get; set; }

        [Option(HelpText = "Logging to file: default is [ExePath]\\Logs\\yyyyMMdd.csv")]
        public override string LogFile { get; set; }

        [Option(HelpText = "Logging to console")]
        public override bool Verbose { get; set; }
    }

}
