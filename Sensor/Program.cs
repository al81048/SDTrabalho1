//sensor desenvolvido
using System;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace SensorApp
{
    class Program
    {
        // Variáveis partilhadas
        static TcpClient client;
        static NetworkStream stream;
        static string sensorId;
        static string tiposDados; // Guardado a nível de classe para a validação poder ler
        static bool aTrabalhar = true;

        // Mutex da Aula 3 para proteger o envio de dados
        static Mutex socketMutex = new Mutex();

        static void Main(string[] args)
        {
            Console.WriteLine("=== SENSOR ONE HEALTH ===");

            // REQUISITO 1: Receber o IP do Gateway
            Console.Write("IP da Gateway (ex: 127.0.0.1): ");
            string ipGateway = Console.ReadLine();

            // REQUISITO 2 e 3: Identificar ID e tipos de dados
            Console.Write("ID deste Sensor (ex: S101): ");
            sensorId = Console.ReadLine();
            Console.Write("Tipos de dados suportados (ex: TEMP,RUIDO): ");
            tiposDados = Console.ReadLine();

            try
            {
                // REQUISITO: Estabelecer ligação
                client = new TcpClient(ipGateway, 5000);
                stream = client.GetStream();
                Console.WriteLine("\n[+] Ligado à Gateway com sucesso!");

                // Envia o registo inicial (A nossa primeira mensagem do protocolo)
                EnviarMensagem($"HELLO|{sensorId}|{tiposDados}");

                // REQUISITO 6: Heartbeat (Thread em segundo plano)
                Thread threadHeartbeat = new Thread(EnviarHeartbeat);
                threadHeartbeat.Start();

                // REQUISITO 8: Interface de texto simples (Menu)
                while (aTrabalhar)
                {
                    Console.WriteLine("\n-- MENU --");
                    Console.WriteLine("1. Enviar Medição (Dados)");
                    Console.WriteLine("2. Pedir Stream de Vídeo");
                    Console.WriteLine("0. Sair");
                    Console.Write("Opção: ");
                    string opcao = Console.ReadLine();

                    if (opcao == "1")
                    {
                        Console.Write("Tipo de dado (ex: TEMP): ");
                        string tipo = Console.ReadLine();

                        // MELHORIA 1: Validação! Verifica se o tipo introduzido está na lista autorizada
                        if (tiposDados.Contains(tipo))
                        {
                            Console.Write("Valor (ex: 25.4): ");
                            string valor = Console.ReadLine();

                            // REQUISITO 4: Enviar medições
                            EnviarMensagem($"DATA|{sensorId}|{tipo}|{valor}");
                        }
                        else
                        {
                            Console.WriteLine($"\n[AVISO] Tipo de dado inválido! Este sensor só suporta: {tiposDados}");
                        }
                    }
                    else if (opcao == "2")
                    {
                        // REQUISITO 5: Enviar necessidade de stream de vídeo
                        EnviarMensagem($"VIDEO|{sensorId}|START");
                    }
                    else if (opcao == "0")
                    {
                        aTrabalhar = false; // Avisa o ciclo do Menu e o ciclo do Heartbeat para pararem
                        EnviarMensagem($"QUIT|{sensorId}");

                        Console.WriteLine("\nA encerrar o sensor em segurança. A aguardar a paragem do Heartbeat...");

                        // MELHORIA 2: Espera educadamente que a Thread do Heartbeat termine antes de fechar a rede
                        threadHeartbeat.Join();
                    }
                }

                // REQUISITO 7: Terminar comunicação corretamente
                stream.Close();
                client.Close();
                Console.WriteLine("[+] Sensor desligado com sucesso.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro: {ex.Message}");
            }
        }

        // Função central para enviar mensagens (Protegida com Mutex!)
        static void EnviarMensagem(string mensagem)
        {
            // O Mutex garante que o Heartbeat e o Menu não enviam mensagens ao mesmo exato milissegundo
            socketMutex.WaitOne();
            try
            {
                byte[] data = Encoding.UTF8.GetBytes(mensagem);
                stream.Write(data, 0, data.Length);
                Console.WriteLine($"\n[Enviado]: {mensagem}");
            }
            finally
            {
                socketMutex.ReleaseMutex();
            }
        }

        // Função que corre na Thread secundária
        static void EnviarHeartbeat()
        {
            while (aTrabalhar)
            {
                // Espera 10 segundos
                Thread.Sleep(10000);

                // Antes de enviar o PING, confirma se o programa ainda está a trabalhar
                if (aTrabalhar)
                {
                    EnviarMensagem($"PING|{sensorId}");
                }
            }
        }
    }
}
