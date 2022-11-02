import redis
import pymongo
from pymongo.server_api import ServerApi
import json
from bson.objectid import ObjectId
import datetime



#Conecção Mongo
client = pymongo.MongoClient(
    "mongodb+srv://desafio:pri123@cluster0.uchv6.mongodb.net/?retryWrites=true&w=majority",
    server_api=ServerApi('1'))
db = client.test

global mydb
mydb = client.mercadoLivre


#Conecção Redis
conectar = redis.Redis(host='redis-13542.c275.us-east-1-4.ec2.cloud.redislabs.com',
                   port=13542,
                   password='qR58kVZOGUkWSQ5M5WTs3bCZ3mwMAujk')
""" conectar = redis.Redis(host='redis-13733.c10.us-east-1-2.ec2.cloud.redislabs.com',
                   port=13733,
                   password='bdnr123') """

""" conR.set('user:name', 'priscila') """
######USUÁRIO######


##Insert##
def insertUser(nome, cpf, cep, estado, rua, email, telefone):
    mycol = mydb.usuario
    print("\n######INSERT######")
    mydict = {
        "nome": nome,
        "cpf": cpf,
        "endereco": {
            "cep": cep,
            "estado": estado,
            "rua": rua,
        },
        "telefone": telefone,
        "email": email
    }
    x = mycol.insert_one(mydict)
    print(f"Usuario gerado com sucesso! com o id -{x.inserted_id}")


##Update##
def updateUser(idAlvo, nome, cpf, endereco, email, telefone):
    global mydb
    mycol = mydb.usuario
    print("\n######UPDATE######")
    objInstance = ObjectId(idAlvo)
    myquery = {"_id": objInstance}
    newValues = {
        "$set": {
            "nome": nome,
            "cpf": cpf,
            "endereco": endereco,
            "telefone": telefone,
            "email": email
        }
    }
    mycol.update_one(myquery, newValues)
    print(newValues.inserted_id)


##Delete##
def deleteUser(user):
    global mydb
    mycol = mydb.usuario
    print("\n######DELETE######")
    objInstance = ObjectId(user)
    myquery = {"_id": objInstance}
    mycol.delete_one(myquery)
    print(mycol.deleted_id)


######VENDEDOR######


##Insert##
def insertVendedor(nome, email, matricula):
    global mydb
    mycol = mydb.vendedor
    print("\n######INSERT VENDEDOR######")
    mydict = {"nome": nome, "matricula": matricula, "email": email}
    x = mycol.insert_one(mydict)
    print(x.inserted_id)


##Update##
def updateVendedor(idAlvo, nome, email):
    global mydb
    mycol = mydb.vendedor
    print("\n######UPDATE VENDEDOR######")
    objInstance = ObjectId(idAlvo)
    myquery = {"_id": objInstance}
    newValues = {"$set": {"nome": nome, "email": email}}
    mycol.update_one(myquery, newValues)
    print("Vendedor Atualizado com sucesso!")


##Delete##
def deleteVendedor(alvo):
    global mydb
    vendedorColumn = mydb.vendedor
    print("\n######DELETE######")
    objInstance = ObjectId(alvo)
    myquery = {"_id": objInstance}
    vendedorColumn.delete_one(myquery)
    print(alvo, "Vendedor deletado com sucesso")


######PRODUTO######


##Insert##
def insertProduto(nomeProdut, preco, descricao, vendedor, quantidade):
    global mydb
    mycol = mydb.produto
    print("\n######INSERT######")
    mydict = {
        "nome": nomeProdut,
        "descricao": descricao,
        "preco": preco,
        "quantidade": quantidade,
        "vendedor": {
            "nome": vendedor,
        }
    }
    x = mycol.insert_one(mydict)
    print(x.inserted_id)


##Update##
def updateProduto(idAlvo, nome, preco, descricao):
    global mydb
    mycol = mydb.produto
    objInstance = ObjectId(idAlvo)
    myquery = {"_id": objInstance}
    newValues = {
        "$set": {
            "nome": nome,
            "preco": preco,
            "descricao": descricao
        }
    }
    mycol.update_one(myquery, newValues)
    print(f"Produto {idAlvo} atualizado")


##Delete##
def deleteProduto(alvo):
    global mydb
    produtoColumn = mydb.produto
    objInstance = ObjectId(alvo)
    myquery = {"_id": objInstance}
    produtoColumn.delete_one(myquery)
    print(f"Produto {alvo} Deletado")


######COMPRA######


##Insert##
def insertCompra(usuario, pagamento, produto):
    global mydb
    columnCompra = mydb.compra
    print("\n######INSERT COMPRA######")

    compra = {
        "usuario": {
            "nome": usuario
        },
        "produto": [{
            "nome": produto
        }],
        "pagamento": pagamento,
        "data_compra": datetime.datetime.now()
    }
    x = columnCompra.insert_one(compra)
    print(x.inserted_id)


def updateCompraItem(idAlvo, produto):
    global mydb
    columnCompra = mydb.compra
    objInstance = ObjectId(idAlvo)
    myquery = {"_id": objInstance}
    updateCompra = {"$addToSet": {"produto": {"nome": produto}}}
    columnCompra.update_one(myquery, updateCompra)


##Update##
def updateCompra(idAlvo, rua, cep, pais):
    global mydb
    mycol = mydb.compra
    objInstance = ObjectId(idAlvo)
    myquery = {"_id": objInstance}
    newValues = {
        "$addToSet": {
            "endereço": {
                "rua": rua,
                "cep": cep,
                "pais": pais
            }
        }
    }
    mycol.update_one(myquery, newValues)
    print("Endereço Atualizado!")


##Delete##
def deleteCompra(alvo):
    global mydb
    mycol = mydb.compra
    objInstance = ObjectId(alvo)
    myquery = {"_id": objInstance}
    mycol.delete_one(myquery)


######QUERYS######


def findSortUser():
    global mydb
    userColumn = mydb.usuario
    mydoc = userColumn.find({}, {
        "nome": 1,
        "_id": 1,
    }).sort("nome")
    for result in mydoc:
        print(result)





def findQueryVendedor(alvo):
    global mydb
    vendedorColumn = mydb.vendedor
    myquery = {"nome": {"$eq": alvo}}
    mydoc = vendedorColumn.find(myquery)
    for result in mydoc:
        print(result)


def findSortVendedores():
    global mydb
    vendedorColumn = mydb.vendedor
    mydoc = vendedorColumn.find({}, {
        "nome": 1,
        "_id": 1,
    }).sort("nome")
    for result in mydoc:
        print(result)


def findQueryProduto(alvo):
    global mydb
    produtoColumn = mydb.produto
    myquery = {"nome": {"$eq": alvo}}
    mydoc = produtoColumn.find(myquery)
    for result in mydoc:
        print(result)


def findSortCompras():
    global mydb
    comprasColumn = mydb.compra
    mydoc = comprasColumn.find({}, {
        "usuario": {
            "nome": 1
        },
        "produto": {
            "nome": 1,
            "preco": 1,
        }
    }).sort("nome")
    for result in mydoc:
        print(result)


def selectuser():
    
    conUser = mydb.usuario
    query2 = conUser.find({}, { "_id": 1, "nome": 1 })
    for x in query2:
        print(x)
    alvo = input("Digite o id: ")
    acharId = ObjectId(alvo)
    query = conUser.find({"_id":{"$eq":acharId}}, {"_id": 0})

    for user in query: print(user)

def main():
    inicializacao = True
    print("""
    1 - Inserir Usuario
    2 - Selecionar Usuario
    3 - Deletar Usuario
    4 - Atualizar Usuario
    5 - Selecionar Todos
    6 -
    7 -
    8 -
    9 -
    10 -
    X - Sair
    """)
    while inicializacao:
        select = input("selecione a opção: ")
        match select:
            case "1":
                nome = input("Digite o nome: ")
                email = input("Digite o email: ")
                cep = input("Digite o cep: ")
                estado = input("Digite o estado: ")
                rua = input("Digite a rua: ")
                telefone = input("Digite o telefone: ")
                cpf = input("Digite o CPF: ")
                insertUser(nome, cpf, cep, estado, rua, email, telefone)
            case "2":
                selectuser()
            case "3":
                idAlvo = input("Selecione o id: ")
                deleteUser(idAlvo)
            case "X" | "x":
                inicializacao = False
            case "4":
                idAlvo = input("Selecione o id: ")
                nome = input("Atualize o nome: ")
                email = input("Atualize o email: ")
                cep = input("Atualize o cep: ")
                estado = input("Atualize o estado: ")
                rua = input("Atualize a rua: ")
                telefone = input("Atualize o telefone: ")
                cpf = input("Atualize o CPF: ")
                updateUser(nome, cpf, cep, estado, rua, email, telefone)
            case "5":
               findSortUser()
                


#main()

# MongoDB -> Redis ( Usuario criar uma Wish List ) ✔
# Reids -> Manipular a Wish List do usuario
# Wish List -> MongoDB

# CHAVE
# fav:ID

def inserirUsuarioRedis(parametro):
    userColumn = mydb.usuario
    favId = ObjectId(parametro)
    mydoc = userColumn.find({
        # papel na gaveta - corresponder a data 27/04/2001
        "_id": {"$eq": favId}
    }, {
        "_id": 1,
    })
    for x in mydoc:
        favoritos = []
        conectar.hset(f'fav:' + str(x["_id"]), "favoritos", json.dumps(favoritos))

def findSortProdutos():
    produtosColumn = mydb.produto
    select = produtosColumn.find({}, {"_id": 1, "nome": 1})
    for itens in select:
        print(itens)

def inserirFavoritosRedis(idUser):
    findSortProdutos()
    selecionarProduto = input("Id do produto acima: ")
    userColumn = mydb.produto
    favId = ObjectId(selecionarProduto)
    mydoc = userColumn.find({
        # papel na gaveta - corresponder a data 27/04/2001
        "_id": {"$eq": favId}
    }, {
        "nome": 1,
        "_id": 0
    }) 
    favRedis = json.loads(conectar.hget(f'fav:' + idUser, "favoritos"))
    listaVazia = []
    for itensJaExistente in favRedis:
        listaVazia.append(itensJaExistente)
    for nomeProduto in mydoc:
        listaVazia.append({
            "Nome" : nomeProduto["nome"],
            "Preço": nomeProduto["preco"]})
    conectar.hset(f'fav:' + idUser, "favoritos", json.dumps(listaVazia))


def sincronizacaoRedisMongo(idUser):
    getUsuario = json.loads(conectar.hget(f'fav:' + idUser, 'favoritos'))
    idObject = ObjectId(idUser)
    gavetaUser = mydb.usuario
    findQueryUsuario = ({"_id": idObject})
    for produto in getUsuario:
        insertProduto = {"$addToSet": {"favoritos": produto}}
        gavetaUser.update_one(findQueryUsuario, insertProduto )


sincronizacaoRedisMongo("632a5e7b099a52557e0f24aa")
