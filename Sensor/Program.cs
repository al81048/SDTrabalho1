using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client; // Biblioteca do RabbitMQ v7+

namespace SensorApp
{
    class Program
    {
        static IConnection connection;
        static IChannel channel;

        static string sensorId;
        static string tiposDados;
        static bool aTrabalhar = true;

        static SemaphoreSlim publishLock = new SemaphoreSlim(1, 1);

        static async Task Main(string[] args)
        {
            Console.WriteLine("=== SENSOR ONE HEALTH (PUB/SUB) ===");

            Console.Write("IP do RabbitMQ (ex: localhost): ");
            string ipRabbit = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(ipRabbit)) ipRabbit = "localhost";

            Console.Write("ID deste Sensor (ex: S101): ");
            sensorId = Console.ReadLine();
            Console.Write("Tipos de dados suportados (ex: TEMP,RUIDO): ");
            tiposDados = Console.ReadLine();

            try
            {
                var factory = new ConnectionFactory { HostName = ipRabbit };
                connection = await factory.CreateConnectionAsync();
                channel = await connection.CreateChannelAsync();

                // Declara o Placard (Exchange)
                await channel.ExchangeDeclareAsync(exchange: "topic_sensores", type: "topic");

                Console.WriteLine("\n[+] Ligado ao RabbitMQ Broker com sucesso!");

                await EnviarMensagem($"HELLO|{sensorId}|{tiposDados}");

                _ = Task.Run(EnviarHeartbeat);

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

                        if (tiposDados.Contains(tipo))
                        {
                            Console.Write("Valor (ex: 25.4): ");
                            string valor = Console.ReadLine();
                            await EnviarMensagem($"DATA|{sensorId}|{tipo}|{valor}");
                        }
                        else
                        {
                            Console.WriteLine($"\n[AVISO] Tipo de dado inválido! Este sensor só suporta: {tiposDados}");
                        }
                    }
                    else if (opcao == "2")
                    {
                        Console.WriteLine("\n[EDGE COMPUTING] A ligar câmara e a iniciar análise local com IA...");

                        // Simula o tempo de processamento de imagem na "borda" (2.5 segundos)
                        await Task.Delay(2500);

                        // Eventos alinhados com o conceito "One Health" (Saúde Única urbana)
                        string[] eventos = {
                            "ALERTA_INCENDIO_FUMO",
                            "ALERTA_AJUNTAMENTO_ANORMAL",
                            "PESSOA_CAIDA_RUA",
                            "FLUXO_NORMAL_SEM_RISCO"
                    };

                        // O "Modelo de IA" escolhe um evento aleatório baseado no vídeo
                        Random rnd = new Random();
                        string eventoDetetado = eventos[rnd.Next(eventos.Length)];

                        Console.WriteLine($"[CÂMARA] Processamento de frames concluído. Extração: {eventoDetetado}");

                        // O Sensor atira apenas o ALERTA em texto (Metadados) para o RabbitMQ
                        await EnviarMensagem($"DATA|{sensorId}|CAMERA|{eventoDetetado}");
                    }
                    else if (opcao == "0")
                    {
                        aTrabalhar = false;
                        await EnviarMensagem($"QUIT|{sensorId}");
                        Console.WriteLine("\nA encerrar o sensor em segurança...");
                        await Task.Delay(1000);
                    }
                }

                await channel.CloseAsync();
                await connection.CloseAsync();
                Console.WriteLine("[+] Sensor desligado com sucesso.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro crítico: {ex.Message}");
            }
        }

        static async Task EnviarMensagem(string mensagem)
        {
            await publishLock.WaitAsync();
            try
            {
                byte[] data = Encoding.UTF8.GetBytes(mensagem);
                string routingKey = $"sensor.{sensorId}";

                await channel.BasicPublishAsync(
                    exchange: "topic_sensores",
                    routingKey: routingKey,
                    body: data,
                    mandatory: false);

                Console.WriteLine($"\n[Publicado no RabbitMQ]: {mensagem}");
            }
            finally
            {
                publishLock.Release();
            }
        }

        static async Task EnviarHeartbeat()
        {
            while (aTrabalhar)
            {
                await Task.Delay(10000);
                if (aTrabalhar)
                {
                    await EnviarMensagem($"PING|{sensorId}");
                }
            }
        }
    }
}