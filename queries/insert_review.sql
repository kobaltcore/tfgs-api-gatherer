-- :name insert_review :insert
insert into review (author, text, date, version, game) values (:author, :text, :date, :version, :game)
