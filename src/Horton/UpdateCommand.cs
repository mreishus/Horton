﻿using System;
using System.Collections.Generic;
using System.Linq;
using Horton.SqlServer;

namespace Horton
{
    internal class UpdateCommand : HortonCommand
    {
        public override void Execute(HortonOptions options)
        {
            var prevColor = Console.ForegroundColor;
            using (var schemaInfo = new SchemaInfo(options))
            {
                schemaInfo.InitializeTable();

                var loader = new FileLoader(options.MigrationsDirectoryPath);
                loader.LoadAllFiles();

                if (!options.Unattend)
                {
                    Console.WriteLine("=== Info ===");
                    Console.WriteLine();
                    Console.WriteLine("The following scripts will execute...");
                }

                var toExecute = new List<ScriptFile>();
                bool willExecuteMigrations = true;

                foreach (var file in loader.Files)
                {
                    var existingRecord = schemaInfo.AppliedMigrations.SingleOrDefault(x => x.FileNameMD5Hash == file.FileNameHash);
                    if (existingRecord != null)
                    {
                        if (!file.IsDesiredState && file.ContentMatches(existingRecord.ContentSHA1Hash))
                        {
                            continue;
                        }
                        if (file.ConflictOnContent)
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine($"\nCONFLICT: The script \"{file.FileName}\" has changed since it was applied on \"{existingRecord.AppliedUTC.ToString("yyyy-MM-dd HH:mm:ss.ff")}\".");
                            Console.ForegroundColor = prevColor;
                            willExecuteMigrations = false;
                            continue;
                        }
                    }
                    if (!options.Unattend)
                    {
                        Console.ForegroundColor = ConsoleColor.DarkGreen;
                        Console.WriteLine($"\"{file.FileName}\" will execute on UPDATE.");
                        Console.ForegroundColor = prevColor;
                    }
                    toExecute.Add(file);
                }

                if (!willExecuteMigrations)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"\nWARNING! Migrations will not execute until conflicts are resolved.");
                    Console.ForegroundColor = prevColor;
                    return;
                }

                if (!options.Unattend && toExecute.Any())
                {
                    Console.WriteLine($"\nAbout to execute {toExecute.Count} scripts. Press 'y' to continue.");
                    var c = Console.ReadKey();
                    Console.WriteLine();
                    if (c.KeyChar != 'y' && c.KeyChar != 'Y')
                    {
                        Console.WriteLine("Aborting...");
                        return;
                    }
                }

                foreach (var file in toExecute)
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.Write($"Applying ");
                    Console.ResetColor();
                    Console.Write($"\"{file.FileName}\"... ");
                    schemaInfo.ApplyMigration(file);
                    Console.WriteLine("done.");
                    Console.ForegroundColor = prevColor;
                }

                Console.WriteLine();
                Console.WriteLine("Finished.");
            }
        }
    }
}