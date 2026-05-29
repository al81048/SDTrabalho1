using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client; // A biblioteca mágica que instalaste

namespace SensorApp
{
    class Program
    {
        // Variáveis partilhadas do RabbitMQ
        static IConnection connection;
        static IChannel channel;

        static string sensorId;
        static string tiposDados;
        static bool aTrabalhar = true;

        // Substituímos o Mutex pelo SemaphoreSlim (a versão segura para código Assíncrono)
        static SemaphoreSlim publishLock = new SemaphoreSlim(1, 1);

        // ATENÇÃO: O Main agora é 'async Task' por causa do RabbitMQ v7
        static async Task Main(string[] args)
        {
            Console.WriteLine("=== SENSOR ONE HEALTH (PUB/SUB) ===");

            // Já não precisamos do IP do Gateway, precisamos do IP do RabbitMQ!
            Console.Write("IP do RabbitMQ (ex: localhost): ");
            string ipRabbit = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(ipRabbit)) ipRabbit = "localhost";

            Console.Write("ID deste Sensor (ex: S101): ");
            sensorId = Console.ReadLine();
            Console.Write("Tipos de dados suportados (ex: TEMP,RUIDO): ");
            tiposDados = Console.ReadLine();

            try
            {
                // 1. Criar ligação ao RabbitMQ
                var factory = new ConnectionFactory { HostName = ipRabbit };
                connection = await factory.CreateConnectionAsync();
                channel = await connection.CreateChannelAsync();

                // 2. Declarar o "Placard de Anúncios" (Exchange)
                // Usamos o tipo "topic" para que a Gateway possa escolher que sensores quer ouvir
                await channel.ExchangeDeclareAsync(exchange: "topic_sensores", type: "topic");

                Console.WriteLine("\n[+] Ligado ao RabbitMQ Broker com sucesso!");

                // Envia o registo inicial
                await EnviarMensagem($"HELLO|{sensorId}|{tiposDados}");

                // Inicia o Heartbeat em pano de fundo (usando Task em vez de Thread)
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
                        await EnviarMensagem($"VIDEO|{sensorId}|START");
                    }
                    else if (opcao == "0")
                    {
                        aTrabalhar = false;
                        await EnviarMensagem($"QUIT|{sensorId}");

                        Console.WriteLine("\nA encerrar o sensor em segurança...");
                        await Task.Delay(1000); // Dá 1 segundo para o último heartbeat cancelar
                    }
                }

                // Fecha a ligação graciosamente
                await channel.CloseAsync();
                await connection.CloseAsync();
                Console.WriteLine("[+] Sensor desligado com sucesso.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro crítico: {ex.Message}");
            }
        }

        // Função de envio atualizada para atirar a mensagem para o RabbitMQ
        static async Task EnviarMensagem(string mensagem)
        {
            await publishLock.WaitAsync();
            try
            {
                byte[] data = Encoding.UTF8.GetBytes(mensagem);

                // Publicamos no Exchange com uma "Routing Key" (etiqueta) específica para este sensor
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

        // Heartbeat adaptado para Task
        static async Task EnviarHeartbeat()
        {
            while (aTrabalhar)
            {
                await Task.Delay(10000); // Espera 10 segundos
                if (aTrabalhar)
                {
                    await EnviarMensagem($"PING|{sensorId}");
                }
            }
        }
    }
}