// Importa funcionalidades básicas do C#
using System;
// Importa a biblioteca de Sockets para criar ligações TCP diretas (usadas para o Servidor e RPC)
using System.Net.Sockets;
// Importa funcionalidades de codificação para transformar bytes em texto (UTF-8)
using System.Text;
// Importa o suporte para Tarefas Assíncronas (Task), necessário para redes modernas e RabbitMQ v7
using System.Threading.Tasks;
// Importa a biblioteca base do RabbitMQ
using RabbitMQ.Client;
// Importa os eventos do RabbitMQ para podermos criar um "escutador" que reage quando chegam mensagens
using RabbitMQ.Client.Events;

namespace GatewayApp
{
    class Program
    {
        // Define o endereço IP do Servidor Central para onde a Gateway vai reencaminhar os dados
        private static string serverIP = "127.0.0.1";
        // Define a porta TCP onde o Servidor Central está à escuta
        private static int serverPort = 9000;

        // O Main é 'async Task' porque a nova versão do RabbitMQ (v7) exige programação assíncrona para não bloquear o sistema
        static async Task Main(string[] args)
        {
            Console.WriteLine("=== GATEWAY ONE HEALTH (PUB/SUB PROXY) ===");

            try
            {
                // 1. Configurar e ligar ao RabbitMQ Broker (O Carteiro)
                // Criamos a "fábrica" de conexões a apontar para a nossa máquina local
                var factory = new ConnectionFactory { HostName = "127.0.0.1" };
                // Estabelece a ligação física ao RabbitMQ
                using var connection = await factory.CreateConnectionAsync();
                // Cria um canal de comunicação por onde as mensagens vão fluir
                using var channel = await connection.CreateChannelAsync();

                // 2. Garantir que o "Placard" (Exchange) existe
                // O tipo "topic" permite usar regras de encaminhamento (routing keys) com wildcards (asteriscos/cardinais)
                await channel.ExchangeDeclareAsync(exchange: "topic_sensores", type: "topic");

                // 3. Criar a "Caixa de Correio" (Fila) da Gateway
                // Deixamos o nome vazio ("") para o RabbitMQ gerar um nome aleatório.
                // exclusive = true e autoDelete = true garantem que a fila é apagada automaticamente se a Gateway for abaixo
                var queueDeclareResult = await channel.QueueDeclareAsync(queue: "", durable: false, exclusive: true, autoDelete: true);
                string queueName = queueDeclareResult.QueueName;

                // 4. O Binding: A regra mágica do Pub/Sub
                // Dizemos ao RabbitMQ: "Liga a minha fila ao placard 'topic_sensores' e manda para lá TUDO o que comece por 'sensor.'"
                // O cardinal '#' significa "qualquer coisa a seguir". Apanha sensor.S101, sensor.S102, etc.
                await channel.QueueBindAsync(queue: queueName, exchange: "topic_sensores", routingKey: "sensor.#");

                Console.WriteLine("[+] Gateway ligada ao RabbitMQ com sucesso!");
                Console.WriteLine("[+] À escuta de mensagens dos Sensores... (Pressione Ctrl+C para sair)");

                // 5. Configurar o mecanismo de escuta ativa (Consumidor)
                // Criamos um consumidor assíncrono associado ao nosso canal
                var consumer = new AsyncEventingBasicConsumer(channel);

                // Definimos o que acontece sempre que uma mensagem cai na nossa fila
                consumer.ReceivedAsync += async (model, ea) =>
                {
                    // Transforma os bytes puros recebidos numa string legível usando UTF-8
                    byte[] body = ea.Body.ToArray();
                    string msgRecebida = Encoding.UTF8.GetString(body);
                    Console.WriteLine($"\n[RabbitMQ -> Gateway]: {msgRecebida}");

                    // ---------------------------------------------------------
                    // FASE 1 TP2: INTERCEPTAR E PRÉ-PROCESSAR DADOS (RPC)
                    // ---------------------------------------------------------
                    // Divide a mensagem em pedaços usando o separador "|"
                    string[] parts = msgRecebida.Split('|');

                    // Só processamos a mensagem se ela for um envio de dados (DATA) e tiver a estrutura correta
                    if (parts.Length >= 4 && parts[0] == "DATA")
                    {
                        string sensorId = parts[1]; // Ex: S101
                        string tipo = parts[2];     // Ex: TEMP ou CAMERA
                        string valorBruto = parts[3]; // Ex: 39 ou ALERTA_FUMO

                        // Chamada RPC (Remote Procedure Call): A Gateway atua como cliente e pede ao Python para limpar o dado
                        string valorLimpo = ChamarPreProcessamentoRPC(tipo, valorBruto);

                        // Reconstrói a mensagem original, mas substitui o valor bruto pelo valor formatado pelo Python
                        msgRecebida = $"DATA|{sensorId}|{tipo}|{valorLimpo}";

                        Console.WriteLine($"[RPC Pré-Processamento] Valor convertido: {valorBruto} -> {valorLimpo}");
                    }
                    // ---------------------------------------------------------

                    // Depois da mensagem estar limpa e validada, enviamos via ligação direta TCP para o Servidor Central
                    EncaminharParaServidor(msgRecebida);

                    // Avisa o sistema que esta tarefa (processar a mensagem atual) foi concluída
                    await Task.CompletedTask;
                };

                // Inicia oficialmente o consumo. autoAck = true diz ao RabbitMQ para apagar a mensagem da fila assim que a entregar
                await channel.BasicConsumeAsync(queue: queueName, autoAck: true, consumer: consumer);

                // Mantém o programa da Gateway a correr infinitamente. Sem isto, a consola abria e fechava logo.
                await Task.Delay(-1);
            }
            catch (Exception ex)
            {
                // Apanha erros gerais na ligação ao RabbitMQ
                Console.WriteLine($"Erro crítico na Gateway: {ex.Message}");
            }
        }

        // Função responsável por fazer a ponte entre a Gateway e o Servidor Central
        static void EncaminharParaServidor(string mensagem)
        {
            try
            {
                // Cria um Cliente TCP temporário para ligar ao Servidor Central (Porta 9000)
                using (TcpClient serverClient = new TcpClient(serverIP, serverPort))
                // Abre o túnel para enviar os dados
                using (NetworkStream serverStream = serverClient.GetStream())
                {
                    // Converte a mensagem formatada para bytes
                    byte[] data = Encoding.UTF8.GetBytes(mensagem);
                    // Escreve os bytes na rede em direção ao Servidor
                    serverStream.Write(data, 0, data.Length);
                    Console.WriteLine("[Gateway -> Servidor]: Encaminhado com sucesso.");
                }
            }
            catch
            {
                // Se o Servidor Central estiver desligado, a Gateway avisa-nos, mas não vai abaixo
                Console.WriteLine("[ERRO] Não foi possível ligar ao Servidor Principal na porta 9000. O Servidor está a correr?");
            }
        }

        // =================================================================================
        // FUNÇÃO FASE 1 TP2: Chamada RPC (Remote Procedure Call) ao serviço Python
        // =================================================================================
        static string ChamarPreProcessamentoRPC(string tipo, string valor)
        {
            try
            {
                // A Gateway liga-se rapidamente ao script Python que está a correr na porta 8001
                using (TcpClient rpcClient = new TcpClient("127.0.0.1", 8001))
                using (NetworkStream stream = rpcClient.GetStream())
                {
                    // Constrói o comando de limpeza (Ex: CLEAN|TEMP|39.5432)
                    string msgRPC = $"CLEAN|{tipo}|{valor}";
                    // Envia os bytes para o Python
                    byte[] dataOut = Encoding.UTF8.GetBytes(msgRPC);
                    stream.Write(dataOut, 0, dataOut.Length);

                    // Fica à espera que o Python processe (limitar casas decimais) e devolva a resposta
                    byte[] dataIn = new byte[1024];
                    int bytesLidos = stream.Read(dataIn, 0, dataIn.Length);

                    // Transforma a resposta do Python em texto e devolve para ser encaminhado
                    return Encoding.UTF8.GetString(dataIn, 0, bytesLidos);
                }
            }
            catch
            {
                // Proteção essencial: Se o serviço Python falhar ou estiver desligado, devolve o valor original
                // Isto garante a Tolerância a Falhas do sistema. Um erro de formatação não pára a cidade inteira.
                return valor;
            }
        }
    }
}