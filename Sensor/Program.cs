using System;
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