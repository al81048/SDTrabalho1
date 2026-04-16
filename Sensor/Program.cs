/*using System;
using System.Net.Sockets;
using System.Text;

namespace SensorApp
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== SENSOR BÁSICO ===");

            // O enunciado pede para receber o IP do Gateway como parâmetro inicial
            Console.Write("Introduza o IP da Gateway (escreve 127.0.0.1 e carrega Enter): ");
            string ipGateway = Console.ReadLine();

            try
            {
                // 1. CONECTAR ao servidor (Gateway) na porta 5000
                TcpClient client = new TcpClient(ipGateway, 5000);
                Console.WriteLine("Conectado à Gateway com sucesso!");

                // 2. ENVIAR DADOS (A nossa mensagem de protocolo)
                // Vamos simular que este é o sensor S101 que mede Temperatura
                string mensagem = "HELLO|S101|TEMP";

                // Os sockets só entendem "Bytes", por isso convertemos o texto
                byte[] data = Encoding.UTF8.GetBytes(mensagem);
                NetworkStream stream = client.GetStream();

                // Escreve os bytes no "tubo" da rede
                stream.Write(data, 0, data.Length);
                Console.WriteLine($"Mensagem enviada: {mensagem}");

                // 3. DESCONECTAR
                stream.Close();
                client.Close();
                Console.WriteLine("Comunicação terminada e Sensor desligado.");
            }
            catch (Exception ex)
            {
                // Se o Gateway não estiver a correr, o programa cai aqui
                Console.WriteLine($"Erro ao tentar ligar: {ex.Message}");
            }

            Console.WriteLine("Pressiona Enter para sair.");
            Console.ReadLine();
        }
    }
}
*/
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
            string tiposDados = Console.ReadLine();

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
                        Console.Write("Valor (ex: 25.4): ");
                        string valor = Console.ReadLine();
                        
                        // REQUISITO 4: Enviar medições
                        EnviarMensagem($"DATA|{sensorId}|{tipo}|{valor}");
                    }
                    else if (opcao == "2")
                    {
                        // REQUISITO 5: Enviar necessidade de stream de vídeo
                        EnviarMensagem($"VIDEO|{sensorId}|START");
                    }
                    else if (opcao == "0")
                    {
                        aTrabalhar = false;
                        EnviarMensagem($"QUIT|{sensorId}");
                    }
                }

                // REQUISITO 7: Terminar comunicação corretamente
                stream.Close();
                client.Close();
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
                
                if (aTrabalhar)
                {
                    EnviarMensagem($"PING|{sensorId}");
                }
            }
        }
    }
}
