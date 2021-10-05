/*
Names and the corresponding average ActualDuration of customers who have received service from an employee 'Emp 2'.
Here I am ignoring the NULL values for calculating the average, because that customer has not completely received service from 'Emp 2'
hence its actual duration wasn't captured
*/
SELECT
    c.name,
    avg(ALL css.actualduration) AS "Average ActualDuration"
FROM
         customer c
    INNER JOIN customerservice         cs ON c.id = cs.cid
    INNER JOIN customerserviceschedule css ON cs.id = css.csid
    INNER JOIN employee                e ON e.id = css.eid
WHERE
    e.name = 'Emp 2'
GROUP BY
    c.name;

/*
Names of all the employees and the corresponding service(s) that each of these employee is allowed to perform
*/
SELECT DISTINCT
    e.name,
    st.name
FROM
         employee e
    INNER JOIN customerserviceschedule css ON e.id = css.eid
    INNER JOIN customerservice         cs ON cs.id = css.csid
    INNER JOIN servicetype             st ON st.id = cs.stid
ORDER BY
    e.name;

/*
Trigger that performs live ETL (Extract Translate and Load) from CustomerService table to CustomerServiceFacts table.
In other words the  trigger shall copy any new record that gets inserted in CustomerService table into CustomerServiceFacts table
as well as updates the ExpectedDuration in CustomerServiceFacts table if any update of ExpectedDuration in CustomerService table takes place
*/
CREATE OR REPLACE TRIGGER copy_record_to_csfactstable AFTER
    INSERT OR UPDATE OF expectedduration ON customerservice
    FOR EACH ROW
BEGIN
    IF inserting THEN
        INSERT INTO customerservicesfacts (
            id,
            cid,
            stid,
            expectedduration
        ) VALUES (
            :new.id,
            :new.cid,
            :new.stid,
            :new.expectedduration
        );

    ELSIF updating THEN
        UPDATE customerservicesfacts
        SET
            expectedduration = :new.expectedduration
        WHERE
                cid = :new.cid
            AND stid = :new.stid;

    END IF;
END;
/

SHOW ERRORS;

/*
Stored Procedure that performs the batch ETL i.e. copies all the data currently in Customer table into CustomerDim table when invoked.
The CustomerDim table has the same composition as the Customer table.
*/
CREATE OR REPLACE PROCEDURE copydatatocustomerdim AS
BEGIN
    DELETE FROM customerdim;

    INSERT INTO customerdim (
        id,
        name,
        address,
        birthdate
    )
        SELECT
            id,
            name,
            address,
            birthdate
        FROM
            customer;

END;
/

EXECUTE copydatatocustomerdim;