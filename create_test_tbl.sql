-- СОЗДАЕМ ТЕСТОВУЮ ТАБЛИЦУ В POSTGRES ДЛЯ МИГРАЦИИ

CREATE TABLE recipes (
  recipe_id INT NOT NULL,
  recipe_name VARCHAR(30) 
);

INSERT INTO recipes 
    (recipe_id, recipe_name) 
VALUES 
    (1, 'Tacos'),
    (2, 'Tomato Soup'),
    (3, 'Grilled Cheese'),
    (4, NULL);
