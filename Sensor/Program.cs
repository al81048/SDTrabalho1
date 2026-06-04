// Importa as funcionalidades básicas do C#
using System;
// Importa as ferramentas de codificação (ex: converter texto em bytes UTF-8)
using System.Text;
// Importa o sistema base de Threads (processos paralelos)
using System.Threading;
// Importa o sistema de Tasks (Tarefas assíncronas, essencial para operações de rede modernas)
using System.Threading.Tasks;
// Importa a biblioteca oficial do RabbitMQ (versão 7 ou superior, que usa programação assíncrona)
using RabbitMQ.Client;

// Define o espaço de nomes do nosso projeto
namespace SensorApp
{
    // Classe principal do Sensor
    class Program
    {
        // Variável que guarda a ligação física principal ao servidor RabbitMQ
        static IConnection connection;
        // Variável que guarda o "Canal" (a via de comunicação lógica por onde passam as mensagens)
        static IChannel channel;

        // Guarda o ID único deste sensor (ex: S101)
        static string sensorId;
        // Guarda os tipos de dados que este sensor consegue ler (ex: TEMP,RUIDO)
        static string tiposDados;
        // Variável de controlo para manter o ciclo do menu a funcionar até o utilizador escolher Sair
        static bool aTrabalhar = true;

        // Semáforo Assíncrono (SemaphoreSlim). Funciona como o Mutex do Servidor, mas para Tasks.
        // Garante que a thread do Menu e a thread do Heartbeat não tentem enviar mensagens ao mesmo tempo,
        // o que faria o canal do RabbitMQ "crashar".
        static SemaphoreSlim publishLock = new SemaphoreSlim(1, 1);

        // Função Main (agora é "async Task" em vez de "void" porque usamos métodos assíncronos do RabbitMQ v7)
        static async Task Main(string[] args)
        {
            // Título na consola
            Console.WriteLine("=== SENSOR ONE HEALTH (PUB/SUB) ===");

            // Pede o IP do computador onde o RabbitMQ está a correr
            Console.Write("IP do RabbitMQ (ex: localhost): ");
            string ipRabbit = Console.ReadLine();
            // Se o utilizador der apenas Enter (vazio), assume "localhost" por defeito
            if (string.IsNullOrWhiteSpace(ipRabbit)) ipRabbit = "localhost";

            // Configuração do perfil deste Sensor
            Console.Write("ID deste Sensor (ex: S101): ");
            sensorId = Console.ReadLine();
            Console.Write("Tipos de dados suportados (ex: TEMP,RUIDO): ");
            tiposDados = Console.ReadLine();

            try
            {
                // Prepara a "fábrica" de conexões indicando onde mora o RabbitMQ
                var factory = new ConnectionFactory { HostName = ipRabbit };

                // Estabelece a ligação ao Broker (os Correios)
                connection = await factory.CreateConnectionAsync();
                // Cria o canal de comunicação dentro dessa ligação
                channel = await connection.CreateChannelAsync();

                // Regra de Ouro do RabbitMQ: Declara o "Placard" (Exchange) onde as mensagens vão ser afixadas.
                // Tipo "topic" permite usar etiquetas dinâmicas e wildcards (como o sensor.# que a Gateway usa).
                await channel.ExchangeDeclareAsync(exchange: "topic_sensores", type: "topic");

                Console.WriteLine("\n[+] Ligado ao RabbitMQ Broker com sucesso!");

                // Envia uma primeira mensagem apenas para avisar a Gateway que este sensor acabou de nascer
                await EnviarMensagem($"HELLO|{sensorId}|{tiposDados}");

                // Inicia o processo de "Heartbeat" (batimento cardíaco) a correr em pano de fundo.
                // O '_' significa "arranca a tarefa e não fiques à espera que ela acabe".
                _ = Task.Run(EnviarHeartbeat);

                // Ciclo infinito do menu principal
                while (aTrabalhar)
                {
                    Console.WriteLine("\n-- MENU --");
                    Console.WriteLine("1. Enviar Medição (Dados)");
                    Console.WriteLine("2. Pedir Stream de Vídeo");
                    Console.WriteLine("0. Sair");
                    Console.Write("Opção: ");

                    // Lê a opção escolhida pelo utilizador
                    string opcao = Console.ReadLine();

                    // Se escolheu enviar uma medição numérica
                    if (opcao == "1")
                    {
                        Console.Write("Tipo de dado (ex: TEMP): ");
                        string tipo = Console.ReadLine(); // Lê o tipo (TEMP, HUM, etc)

                        // Verifica se o tipo introduzido faz parte da lista de tipos que o sensor suporta
                        if (tiposDados.Contains(tipo))
                        {
                            Console.Write("Valor (ex: 25.4): ");
                            string valor = Console.ReadLine(); // Lê o valor

                            // Chama a função para atirar a mensagem estruturada para o RabbitMQ
                            await EnviarMensagem($"DATA|{sensorId}|{tipo}|{valor}");
                        }
                        else
                        {
                            // Se introduziu um tipo inválido (ex: RUIDO num sensor que só lê TEMP), avisa o erro
                            Console.WriteLine($"\n[AVISO] Tipo de dado inválido! Este sensor só suporta: {tiposDados}");
                        }
                    }
                    // A NOSSA GRANDE JOGADA: EDGE COMPUTING EM VEZ DE STREAM DE VÍDEO
                    else if (opcao == "2")
                    {
                        Console.WriteLine("\n[EDGE COMPUTING] A ligar câmara e a iniciar análise local com IA...");

                        // Pausa a execução por 2.5 segundos para simular a IA a "pensar" e analisar os frames do vídeo
                        await Task.Delay(2500);

                        // Lista de possíveis cenários/emergências que a IA da câmara pode detetar (foco no One Health)
                        string[] eventos = {
                            "ALERTA_INCENDIO_FUMO",
                            "ALERTA_AJUNTAMENTO_ANORMAL",
                            "PESSOA_CAIDA_RUA",
                            "FLUXO_NORMAL_SEM_RISCO"
                        };

                        // Cria um gerador de números aleatórios
                        Random rnd = new Random();
                        // Escolhe um dos eventos da lista à sorte (simulando a conclusão da IA)
                        string eventoDetetado = eventos[rnd.Next(eventos.Length)];

                        // Informa o utilizador do que a IA viu
                        Console.WriteLine($"[CÂMARA] Processamento de frames concluído. Extração: {eventoDetetado}");

                        // Em vez de enviar megabytes de vídeo para a rede, envia apenas o alerta em formato texto
                        await EnviarMensagem($"DATA|{sensorId}|CAMERA|{eventoDetetado}");
                    }
                    // Opção para desligar o sensor de forma limpa
                    else if (opcao == "0")
                    {
                        // Quebra o ciclo while
                        aTrabalhar = false;
                        // Envia uma última mensagem para a Gateway saber que o sensor morreu/desligou
                        await EnviarMensagem($"QUIT|{sensorId}");
                        Console.WriteLine("\nA encerrar o sensor em segurança...");
                        // Espera 1 segundo para garantir que a mensagem viaja pela rede antes de fechar o programa
                        await Task.Delay(1000);
                    }
                }

                // Fecha o canal e a ligação física ao RabbitMQ educadamente
                await channel.CloseAsync();
                await connection.CloseAsync();
                Console.WriteLine("[+] Sensor desligado com sucesso.");
            }
            catch (Exception ex)
            {
                // Se o RabbitMQ não estiver a correr ou houver erro de rede, apanha o erro aqui e não explode o ecrã
                Console.WriteLine($"Erro crítico: {ex.Message}");
            }
        }

        // Função responsável por embrulhar e publicar mensagens no RabbitMQ
        static async Task EnviarMensagem(string mensagem)
        {
            // Bloqueia a via (Semáforo). Se o Heartbeat estiver a enviar algo, o Menu espera, e vice-versa.
            await publishLock.WaitAsync();
            try
            {
                // O RabbitMQ não transporta texto, transporta bytes. Convertemos a string para um array de bytes.
                byte[] data = Encoding.UTF8.GetBytes(mensagem);

                // Cria a "Etiqueta" (Routing Key) dinâmica. Exemplo: "sensor.S101"
                // É graças a isto que a Gateway consegue pescar as mensagens com a regra "sensor.#"
                string routingKey = $"sensor.{sensorId}";

                // Função core do Produtor: Publicar a mensagem no Exchange
                await channel.BasicPublishAsync(
                    exchange: "topic_sensores", // O nome do placard
                    routingKey: routingKey,     // A etiqueta da mensagem
                    body: data,                 // O conteúdo real (os bytes)
                    mandatory: false);          // 'false' significa: se não houver Gateways a ouvir, podes deitar a mensagem fora

                // Confirmação visual de que a mensagem seguiu viagem
                Console.WriteLine($"\n[Publicado no RabbitMQ]: {mensagem}");
            }
            finally
            {
                // Super importante: Liberta o semáforo para outras tarefas poderem usar o RabbitMQ
                publishLock.Release();
            }
        }

        // Função que corre infinitamente em pano de fundo para provar que o sensor está vivo
        static async Task EnviarHeartbeat()
        {
            while (aTrabalhar)
            {
                // Espera 10 segundos
                await Task.Delay(10000);

                // Após esperar, verifica se o utilizador não mandou encerrar o programa entretanto
                if (aTrabalhar)
                {
                    // Envia uma mensagem "PING" automática para a rede
                    await EnviarMensagem($"PING|{sensorId}");
                }
            }
        }
    }
}