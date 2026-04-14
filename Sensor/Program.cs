using System;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace SensorApp
{
    class Program
    {
        // Variáveis globais para partilhar entre as threads
        static TcpClient client;
        static NetworkStream stream;
        static string sensorId;
        static bool isRunning = true;

        // Mutex para proteger o acesso ao envio de dados pelo Socket
        static Mutex streamMutex = new Mutex();

        static void Main(string[] args)
        {
            Console.WriteLine("=== SENSOR ONE HEALTH ===");

            // 1. Receber o IP do Gateway
            Console.Write("Introduza o IP da Gateway (ex: 127.0.0.1): ");
            string ipGateway = Console.ReadLine();

            Console.Write("Introduza o ID deste Sensor (ex: S101): ");
            sensorId = Console.ReadLine();

            Console.Write("Introduza os tipos de dados separados por vírgula (ex: TEMP,RUIDO): ");
            string tiposDados = Console.ReadLine();

            try
            {
                // 2. Estabelecer ligação
                client = new TcpClient(ipGateway, 5000);
                stream = client.GetStream();
                Console.WriteLine("\n[+] Ligado à Gateway com sucesso!");

                // 3. Enviar mensagem de registo (HELLO)
                EnviarMensagem($"HELLO|{sensorId}|{tiposDados}");

                // 4. Iniciar a Thread do Heartbeat (ping a cada 10 segundos)
                Thread heartbeatThread = new Thread(new ThreadStart(EnviarHeartbeatPeriodico));
                heartbeatThread.Start();

                // 5. Interface de texto simples para simular envio de dados
                while (isRunning)
                {
                    Console.WriteLine("\n--- MENU SENSOR ---");
                    Console.WriteLine("1. Enviar Medição Ambiental");
                    Console.WriteLine("2. Pedir criação de Stream de Vídeo");
                    Console.WriteLine("0. Sair");
                    Console.Write("Escolha uma opção: ");

                    string opcao = Console.ReadLine();

                    switch (opcao)
                    {
                        case "1":
                            Console.Write("Tipo de dado (ex: TEMP): ");
                            string tipo = Console.ReadLine();
                            Console.Write("Valor (ex: 25.4): ");
                            string valor = Console.ReadLine();

                            // Formato: DATA|ID|TIPO|VALOR
                            EnviarMensagem($"DATA|{sensorId}|{tipo}|{valor}");
                            break;

                        case "2":
                            // Formato simulado para stream de vídeo
                            EnviarMensagem($"VIDEO|{sensorId}|REQUEST_STREAM");
                            Console.WriteLine("Pedido de vídeo enviado!");
                            break;

                        case "0":
                            isRunning = false;
                            EnviarMensagem($"QUIT|{sensorId}");
                            break;

                        default:
                            Console.WriteLine("Opção inválida.");
                            break;
                    }
                }

                // Terminar corretamente a comunicação
                stream.Close();
                client.Close();
                Console.WriteLine("Sensor desligado.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro crítico: {ex.Message}");
            }
        }

        // Função responsável por escrever no Socket de forma segura
        static void EnviarMensagem(string mensagem)
        {
            if (stream != null && stream.CanWrite)
            {
                byte[] data = Encoding.UTF8.GetBytes(mensagem);

                // Bloqueia o acesso. Se o heartbeat estiver a enviar, o menu espera (e vice-versa)
                streamMutex.WaitOne();
                try
                {
                    stream.Write(data, 0, data.Length);
                    Console.WriteLine($"[Enviado]: {mensagem}");
                }
                finally
                {
                    // Liberta sempre o mutex, mesmo que dê erro
                    streamMutex.ReleaseMutex();
                }
            }
        }

        // Função que corre na Thread secundária
        static void EnviarHeartbeatPeriodico()
        {
            while (isRunning)
            {
                // Espera 10 segundos (10000 milissegundos)
                Thread.Sleep(10000);

                if (isRunning)
                {
                    EnviarMensagem($"PING|{sensorId}");
                }
            }
        }
    }
}