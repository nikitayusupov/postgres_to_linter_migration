-- СОЗДАЕМ ТЕСТОВУЮ ТАБЛИЦУ В POSTGRES ДЛЯ МИГРАЦИИ

CREATE TABLE students (
  student_id INT NOT NULL,
  student_name VARCHAR(60) 
);

INSERT INTO students 
    (student_id, student_name) 
VALUES 
    (0, 'Mike'),
    (1, 'Ivan'),
    (2, 'Dima Ivanov'),
    (3, NULL);
