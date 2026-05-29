using System;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks; // Adicionado para suportar Task/Async
using RabbitMQ.Client; // Biblioteca do RabbitMQ
using RabbitMQ.Client.Events; // Adicionado para os eventos do Consumidor

namespace GatewayApp
{
    class Program
    {
        private static string serverIP = "127.0.0.1";
        private static int serverPort = 9000;

        // O Main agora é 'async Task' para suportar a biblioteca moderna do RabbitMQ v7
        static async Task Main(string[] args)
        {
            Console.WriteLine("=== GATEWAY ONE HEALTH (PUB/SUB PROXY) ===");

            try
            {
                // 1. Configurar e ligar ao RabbitMQ Broker no Docker
                var factory = new ConnectionFactory { HostName = "127.0.0.1" };
                using var connection = await factory.CreateConnectionAsync();
                using var channel = await connection.CreateChannelAsync();

                // 2. Garantir que o "Placard" (Exchange) do tipo Topic existe
                await channel.ExchangeDeclareAsync(exchange: "topic_sensores", type: "topic");

                // 3. Criar uma fila exclusiva e automática para esta Gateway
                var queueDeclareResult = await channel.QueueDeclareAsync(queue: "", durable: false, exclusive: true, autoDelete: true);
                string queueName = queueDeclareResult.QueueName;

                // 4. Efetuar o "Bind" via código: Ouvir todas as mensagens que comecem por "sensor."
                await channel.QueueBindAsync(queue: queueName, exchange: "topic_sensores", routingKey: "sensor.#");

                Console.WriteLine("[+] Gateway ligada ao RabbitMQ com sucesso!");
                Console.WriteLine("[+] À escuta de mensagens dos Sensores... (Pressione Ctrl+C para sair)");

                // 5. Configurar o mecanismo de escuta ativa (Consumidor Assíncrono)
                var consumer = new AsyncEventingBasicConsumer(channel);

                consumer.ReceivedAsync += async (model, ea) =>
                {
                    // Desempacota os bytes recebidos do RabbitMQ para string
                    byte[] body = ea.Body.ToArray();
                    string msgRecebida = Encoding.UTF8.GetString(body);
                    Console.WriteLine($"\n[RabbitMQ -> Gateway]: {msgRecebida}");

                    // ---------------------------------------------------------
                    // FASE 1 TP2: INTERCEPTAR E PRÉ-PROCESSAR DADOS (RPC)
                    // ---------------------------------------------------------
                    string[] parts = msgRecebida.Split('|');

                    // Se for uma medição (DATA), fazemos a limpeza no Python antes de enviar
                    if (parts.Length >= 4 && parts[0] == "DATA")
                    {
                        string sensorId = parts[1];
                        string tipo = parts[2];
                        string valorBruto = parts[3];

                        // Manda limpar para o serviço Python (Porta 8001)
                        string valorLimpo = ChamarPreProcessamentoRPC(tipo, valorBruto);

                        // Reconstrói a mensagem para enviar para o Servidor Central (agora com o valor formatado)
                        msgRecebida = $"DATA|{sensorId}|{tipo}|{valorLimpo}";

                        Console.WriteLine($"[RPC Pré-Processamento] Valor convertido: {valorBruto} -> {valorLimpo}");
                    }
                    // ---------------------------------------------------------

                    // Encaminha a mensagem (limpa ou original) para o Servidor Central via TCP (Porta 9000)
                    EncaminharParaServidor(msgRecebida);

                    await Task.CompletedTask;
                };

                // Inicia o consumo efetivo da fila
                await channel.BasicConsumeAsync(queue: queueName, autoAck: true, consumer: consumer);

                // Bloqueia a aplicação para que a Gateway continue permanentemente acordada à escuta
                await Task.Delay(-1);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro crítico na Gateway: {ex.Message}");
            }
        }

        static void EncaminharParaServidor(string mensagem)
        {
            try
            {
                // Abre ligação rápida ao Servidor Central (Porta 9000)
                using (TcpClient serverClient = new TcpClient(serverIP, serverPort))
                using (NetworkStream serverStream = serverClient.GetStream())
                {
                    byte[] data = Encoding.UTF8.GetBytes(mensagem);
                    serverStream.Write(data, 0, data.Length);
                    Console.WriteLine("[Gateway -> Servidor]: Encaminhado com sucesso.");
                }
            }
            catch
            {
                Console.WriteLine("[ERRO] Não foi possível ligar ao Servidor Principal na porta 9000. O Servidor está a correr?");
            }
        }

        // =================================================================================
        // FUNÇÃO FASE 1 TP2: Chamar Serviço de Limpeza Python na Porta 8001
        // =================================================================================
        static string ChamarPreProcessamentoRPC(string tipo, string valor)
        {
            try
            {
                using (TcpClient rpcClient = new TcpClient("127.0.0.1", 8001))
                using (NetworkStream stream = rpcClient.GetStream())
                {
                    string msgRPC = $"CLEAN|{tipo}|{valor}";
                    byte[] dataOut = Encoding.UTF8.GetBytes(msgRPC);
                    stream.Write(dataOut, 0, dataOut.Length);

                    byte[] dataIn = new byte[1024];
                    int bytesLidos = stream.Read(dataIn, 0, dataIn.Length);
                    return Encoding.UTF8.GetString(dataIn, 0, bytesLidos);
                }
            }
            catch
            {
                // Se o Python na porta 8001 falhar/estiver desligado, devolve o valor original 
                // para não bloquear o funcionamento do sistema
                return valor;
            }
        }
    }
}