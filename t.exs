"""
interface Character {
  id: ID!
  name: String!
  friends: [Character]
  appearsIn: [Episode]!
}
type Human implements Character {
  id: ID!
  name: String!
  friends: [Character]
  appearsIn: [Episode]!
  starships: [Starship]
  totalCredits: Int
}
type Droid implements Character {
  id: ID!
  name: String!
  friends: [Character]
  appearsIn: [Episode]!
  primaryFunction: String
}
enum Episode {
  NEWHOPE
  EMPIRE
  JEDI
}
"""
|> Absinthe.Lexer.tokenize()
|> elem(1)
|> :absinthe_parser.parse()
|> IO.inspect(label: "lexed")
