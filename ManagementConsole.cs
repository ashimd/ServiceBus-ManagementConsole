using System;

namespace ServiceBusManagementConsole
{
    class ManagementConsole
    {
        private static string ServiceBusConnectionString = "";

        static void Main(string[] args)
        {
            ManagementHelper helper = new ManagementHelper(ServiceBusConnectionString);

            do
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.Write(">");
                string commandLine = Console.ReadLine();
                Console.ForegroundColor = ConsoleColor.Magenta;
                string[] commands = commandLine.Split(' ');

                try
                {
                    if (commands.Length > 0)
                    {
                        switch (commands[0])
                        {
                            case "createqueue":
                            case "cq":
                                if (commands.Length > 1)
                                {
                                    helper.CreateQueue(commands[1]);
                                }
                                else
                                {
                                    Console.ForegroundColor = ConsoleColor.Yellow;
                                    Console.WriteLine("Queue path not specified.");
                                }
                                break;
                            case "listqueues":
                            case "lq":
                                helper.ListQueues();
                                break;
                            case "getqueue":
                            case "gq":
                                if (commands.Length > 1)
                                {
                                    helper.GetQueue(commands[1]);
                                }
                                else
                                {
                                    Console.ForegroundColor = ConsoleColor.Yellow;
                                    Console.WriteLine("Queue path not specified.");
                                }
                                break;
                            case "deletequeue":
                            case "dq":
                                if (commands.Length > 1)
                                {
                                    helper.DeleteQueue(commands[1]);
                                }
                                else
                                {
                                    Console.ForegroundColor = ConsoleColor.Yellow;
                                    Console.WriteLine("Queue path not specified.");
                                }
                                break;
                            default:
                                break;
                        }
                    }
                }
                catch (Exception)
                {
                    throw;
                }
            } while (true);
        }
    }
}
