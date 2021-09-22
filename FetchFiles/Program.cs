using CommandLine;
using CsvHelper;
using CsvHelper.Configuration;
using NLog;
using NLog.Layouts;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace FetchFiles
{
    class Program
    {
        private static string ExePath;
        private static string RootPath;
        private static string LogPath;
        private static Logger logger;
        private static Stopwatch _watch;
        private static bool ParserError = false;


        static void Main(string[] args)
        {
            ExePath = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            RootPath = ExePath;           
            LogPath = Path.Combine(RootPath, "Logs");


            logger = new Logger("log");
            //var parser = CommandLine.Parser.Default;
            //parser.Settings.CaseSensitive = false;
            var parser = new Parser(config =>
            {
                config.IgnoreUnknownArguments = false;
                config.CaseSensitive = false;
                config.AutoHelp = true;
                config.AutoVersion = true;
                config.HelpWriter = Console.Error;                
            });


            var result = parser.ParseArguments<ExportToDB, ExportToCSV>(args)
                .WithParsed<ExportToDB>(s => RunExportToDB(s))
                .WithParsed<ExportToCSV>(s => RunExportToCSV(s))
                .WithNotParsed(errors => HandleParseError(errors));

            if (!ParserError)
            {
                _watch.Stop();
                logger.Debug($"Application Finished. Elapsed time: {_watch.ElapsedMilliseconds}ms");
            }

#if DEBUG
            Console.WriteLine("Press enter to close...");
            Console.ReadLine();
#endif

        }

        static void LoggerConfigure(Options opts)
        {
            var config = new NLog.Config.LoggingConfiguration();

            // Targets where to log to: File and Console
            var logfile = new NLog.Targets.FileTarget("logfile");

            if (!Directory.Exists(LogPath)) Directory.CreateDirectory(LogPath);

            if (opts.LogFile != null)
            {
                if (Path.GetFileName(opts.LogFile) == opts.LogFile)
                    logfile.FileName = $"{Path.Combine(LogPath, opts.LogFile)}";
                else
                    logfile.FileName = $"{opts.LogFile}";
            }
            else
                logfile.FileName = $"{Path.Combine(LogPath, $"{DateTime.Now.ToString("yyyyMMdd")}.csv")}";

            logfile.MaxArchiveFiles = 60; // max archive file 60
            logfile.ArchiveAboveSize = 1024 * 5000; // max 5MB

            var logconsole = new NLog.Targets.ConsoleTarget("logconsole");
            if (opts.Verbose)
                config.AddRule(LogLevel.Trace, LogLevel.Fatal, logconsole);
            else
                config.AddRule(LogLevel.Error, LogLevel.Fatal, logconsole);

            config.AddRule(LogLevel.Trace, LogLevel.Fatal, logfile);

            // design layout for file log rotation
            CsvLayout layout = new CsvLayout();
            layout.Delimiter = CsvColumnDelimiterMode.Comma;
            layout.Quoting = CsvQuotingMode.Auto;
            layout.Columns.Add(new CsvColumn("Start Time", "${longdate}"));
            layout.Columns.Add(new CsvColumn("Elapsed Time", "${elapsed-time}"));
            layout.Columns.Add(new CsvColumn("Machine Name", "${machinename}"));
            layout.Columns.Add(new CsvColumn("Login", "${windows-identity}"));
            layout.Columns.Add(new CsvColumn("Level", "${uppercase:${level}}"));
            layout.Columns.Add(new CsvColumn("Message", "${message}"));
            layout.Columns.Add(new CsvColumn("Exception", "${exception:format=toString}"));
            logfile.Layout = layout;

            // design layout for console log rotation
            SimpleLayout ConsoleLayout = new SimpleLayout("${longdate}:${message}\n${exception}");
            logconsole.Layout = ConsoleLayout;

            // Apply config           
            NLog.LogManager.Configuration = config;
        }

        static void OptionConfigure(Options opts)
        {
            char[] chars = new char[] { '{', '}' };
            char[] chars2 = new char[] { '[', ']' };

            var FileFilterFunc = opts.FileFilters.Where(x => x.Contains("{"))
                .Select(x => chars.Aggregate(x, (c1, c2) => c1.Replace(c2, '\n')) );

            if (FileFilterFunc.Count() > 0)
            {
                opts.FileFilters = Enumerable.Empty<string>();
                List<string> tmp_FileFilters = new List<string>();

                //{yyyy-MM-dd},{yyyyMMdd},{yyMMdd},{yyyy-MM-dd HH:mm:ss}
                foreach (var func in FileFilterFunc)
                {                    
                    if (func.Contains("["))
                    {
                        string dayextract = func.Split('[', ']')[1];
                        string funcstr = func.Replace("[", "").Replace("]", "").Replace(dayextract,"");
                        string FilterStr = DateTime.Now.AddDays(double.Parse(dayextract)).ToString(funcstr).Replace("\n", "");
                        tmp_FileFilters.Add(FilterStr);
                    }
                    else 
                        tmp_FileFilters.Add(DateTime.Now.ToString(func).Replace("\n",""));                    

                }

                opts.FileFilters = opts.FileFilters.Concat(tmp_FileFilters);
            }


            var DirFilterFunc = opts.DirectoryFilters.Where(x => x.Contains("{"))
                .Select(x => chars.Aggregate(x, (c1, c2) => c1.Replace(c2, '\n')));

            if (DirFilterFunc.Count() > 0)
            {
                opts.DirectoryFilters = Enumerable.Empty<string>();
                List<string> tmp_DirFilters = new List<string>();

                //{yyyy-MM-dd},{yyyyMMdd},{yyMMdd},{yyyy-MM-dd HH:mm:ss}
                foreach (var func in DirFilterFunc)
                {
                    if (func.Contains("["))
                    {
                        string dayextract = func.Split('[', ']')[1];
                        string funcstr = func.Replace("[", "").Replace("]", "").Replace(dayextract, "");
                        string FilterStr = DateTime.Now.AddDays(double.Parse(dayextract)).ToString(funcstr).Replace("\n", "");
                        tmp_DirFilters.Add(FilterStr);
                    }
                    else
                        tmp_DirFilters.Add(DateTime.Now.ToString(func).Replace("\n", ""));

                    //tmp_DirFilters.Add(DateTime.Now.ToString(func).Replace("\n",""));
                }

                opts.DirectoryFilters = opts.DirectoryFilters.Concat(tmp_DirFilters);
            }

        }

        static int RunExportToDB(Options opts)
        {
            var exitCode = 0;

            LoggerConfigure(opts);
            OptionConfigure(opts);

            _watch = new Stopwatch();
            _watch.Start();
            logger.Debug("Application Start");
            IEnumerable<string> FileList = Enumerable.Empty<string>();  
            IEnumerable<string> PathListEnum = Enumerable.Empty<string>();  

            PathListEnum = GetPathList(opts.PathList);
            SearchOption searchOption = SearchOption.AllDirectories;

            if (opts.SearchTopDirOnly)
            {
                searchOption = SearchOption.TopDirectoryOnly;
            }            

            if (PathListEnum.Count() <= 0)
            {
                logger.Debug($"Path not found within critera DirectoryFilter {String.Join(",", opts.DirectoryFilters)}");
            }
            else
            {
                Parallel.ForEach(PathListEnum, path =>
                {
                    try
                    {


                        IEnumerable<string> tmp_FileList = Enumerable.Empty<string>();
                        IEnumerable<string> tmp_DirList = Enumerable.Empty<string>();

                        if (opts.DirectoryFilters.Contains("*"))
                        {
                            tmp_FileList = GetFiles(path, opts.FileFilters.ToArray(), searchOption);
                            foreach (string tmp_file in tmp_FileList) logger.Debug($"File found within criteria : {tmp_file}");
                        }
                        else
                        {
                            tmp_DirList = GetDirs(path, opts.DirectoryFilters.ToArray());
                            foreach (string tmp_dir in tmp_DirList) logger.Debug($"Directory found within criteria : {tmp_dir}");

                            tmp_FileList = GetFiles(tmp_DirList, opts.FileFilters.ToArray(), searchOption);
                            foreach (string tmp_file in tmp_FileList) logger.Debug($"File found within criteria : {tmp_file}");
                        }



                        if (tmp_FileList.Count() > 0) FileList = FileList.Concat(tmp_FileList);
                    }
                    catch (Exception ex)
                    {
                        logger.Error("Error Generate File List", ex);
                    }
                });

                if (FileList.Count() <= 0)
                {
                    logger.Debug($"File not found within critera DirectoryFilter {String.Join(",", opts.DirectoryFilters)} and File Filter {String.Join(",", opts.FileFilters)}");
                }
                else
                {
                    string QueryInsert;

                    using (StreamReader sr = new StreamReader(Path.Combine(RootPath,"QueryInsert.sql")))
                    {
                        QueryInsert = sr.ReadToEnd();
                    }

                    string ConnectionString = $"Data Source={opts.ServerName};Initial Catalog={opts.DBName};Integrated Security=True;Connection Timeout=60;";

                    try
                    {
                        using (SqlConnection conn = new SqlConnection(ConnectionString))
                        {
                            try
                            {
                                conn.Open();
                                if (opts.TruncateTable)
                                {
                                    using (SqlCommand cmd = new SqlCommand())
                                    {
                                        cmd.Connection = conn;
                                        cmd.CommandText = $"IF OBJECT_ID('{opts.TableName}') IS NULL CREATE TABLE {opts.TableName} ([FileName] varchar(max)) ELSE TRUNCATE TABLE {opts.TableName}";
                                        logger.Debug(cmd.CommandText);
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                logger.Error("Create Table", ex);
                            }

                            using (DataTable dt = new DataTable(opts.TableName))
                            {
                                dt.Columns.Add("FileName");

                                logger.Debug("Dump to DataTable...");
                                foreach (var FileName in FileList)
                                {
                                    dt.Rows.Add(FileName);
                                }
                                logger.Debug("Dump to DataTable is Done");

                                logger.Debug("Bulk Insert...");
                                // make sure to enable triggers
                                // more on triggers in next post
                                SqlBulkCopy bulkCopy = new SqlBulkCopy(
                                    conn,
                                    SqlBulkCopyOptions.TableLock |
                                    SqlBulkCopyOptions.FireTriggers |
                                    SqlBulkCopyOptions.UseInternalTransaction
                                    ,
                                    null
                                    );

                                // set the destination table name
                                bulkCopy.DestinationTableName = opts.TableName;

                                // write the data in the "dataTable"
                                bulkCopy.WriteToServer(dt);
                                logger.Debug("Bulk Insert is Done");

                            };

                            conn.Close();
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.Error("Sql", ex);
                    }
                }

            }

            return exitCode;
        }

        static int RunExportToCSV(Options opts)
        {
            var exitCode = 0;

            LoggerConfigure(opts);
            OptionConfigure(opts);


            _watch = new Stopwatch();
            _watch.Start();
            logger.Debug("Application Start");
            IEnumerable<string> FileList = Enumerable.Empty<string>();
            IEnumerable<string> PathListEnum = Enumerable.Empty<string>();

            PathListEnum = GetPathList(opts.PathList);
            if (PathListEnum.Count() <= 0)
            {
                logger.Debug($"Path not found within critera DirectoryFilter {String.Join(",", opts.DirectoryFilters)}");
            }
            else
            {
                Parallel.ForEach(PathListEnum, path =>
                {
                    try
                    {
                        IEnumerable<string> tmp_FileList = Enumerable.Empty<string>();
                        IEnumerable<string> tmp_DirList = Enumerable.Empty<string>();

                        tmp_DirList = GetDirs(path, opts.DirectoryFilters.ToArray());
                        foreach (string tmp_dir in tmp_DirList) logger.Debug($"Directory found within criteria : {tmp_dir}");

                        tmp_FileList = GetFiles(tmp_DirList, opts.FileFilters.ToArray());
                        foreach (string tmp_file in tmp_FileList) logger.Debug($"File found within criteria : {tmp_file}");

                        if (tmp_FileList.Count() > 0) FileList = FileList.Concat(tmp_FileList);
                    }
                    catch (Exception ex)
                    {
                        logger.Error("Error Generate File List", ex);
                    }
                });

                if (FileList.Count() <= 0)
                {
                    logger.Debug($"File not found within critera DirectoryFilter {String.Join(",", opts.DirectoryFilters)} and File Filter {String.Join(",", opts.FileFilters)}");
                }
                else
                {
                    try
                    {
                        using (var writer = new StreamWriter(opts.CsvFile, false))
                        using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                        {
                            logger.Debug($"Writing CSV File... {opts.CsvFile}");

                            csv.WriteField<string>("FileName");
                            csv.NextRecord();
                            foreach (var FileName in FileList)
                            {
                                csv.WriteField<string>(FileName);
                                csv.NextRecord();
                            }
                            logger.Debug("Writing CSV File is done");
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.Error("CsvWriter", ex);
                    }
                }
            }
 

            return exitCode;
        }

        static void HandleParseError(IEnumerable<Error> errs)
        {
            ParserError = true;

            if (errs.Any(x => x is HelpRequestedError || x is VersionRequestedError))
            {
            }
            else
                Console.WriteLine("Parameter unknown, please check the documentation or use parameter '--help' for more information");


        }

        public static IEnumerable<string> GetFiles(string path,
                    string[] searchPatterns,
                    SearchOption searchOption = SearchOption.AllDirectories)
        {

            if (searchPatterns.Length == 0)
            {
                searchPatterns = new string[] { "*" };
            }
            
            return searchPatterns.AsParallel()
                   .SelectMany(searchPattern =>
                          Directory.EnumerateFiles(path, searchPattern, searchOption));

        }

        public static IEnumerable<string> GetFiles(IEnumerable<string> paths,
                    string[] searchPatterns,
                    SearchOption searchOption = SearchOption.AllDirectories)
        {
            //var messages = new ConcurrentBag<string>();

            IEnumerable<string> retval = Enumerable.Empty<string>();

            return paths.AsParallel()
                .SelectMany(path =>
                    GetFiles(path, searchPatterns, searchOption));

            //foreach (var path in paths)
            //{
            //    retval = (retval ?? Enumerable.Empty<string>()).Concat(GetFiles(path, searchPatterns, searchOption) ?? Enumerable.Empty<string>());
            //}
            
            //return retval;

        }

        public static IEnumerable<string> GetDirs(string path,
                    string[] searchPatterns,
                    SearchOption searchOption = SearchOption.AllDirectories)
        {

            if (searchPatterns.Length == 0)
            {
                searchPatterns = new string[] { "*" };
            }

            return searchPatterns.AsParallel()
                   .SelectMany(searchPattern =>
                          Directory.EnumerateDirectories(path, searchPattern, searchOption));

        }
    
        public static IEnumerable<string> GetPathList(string FileName)
        {
            IEnumerable<string> retval = Enumerable.Empty<string>();

            if (File.Exists(FileName)) retval = File.ReadLines(FileName);

            return retval;
        }
    
    }
    public sealed class CsvMap : ClassMap<string>
    {
        public CsvMap()
        {
            Map(m => m);
        }
    }
}
