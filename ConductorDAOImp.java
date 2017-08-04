package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import sys.dao.ConductorDAO;
import sys.model.AimarConductores;
import sys.util.Service;

public class ConductorDAOImp implements ConductorDAO {

    @Override
    public List<AimarConductores> listarConductores() {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AimarConductores conductor = null;
        List<AimarConductores> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT AIMAR_Conductores.Dni_con, "
                    + "AIMAR_Conductores.Abr_con, "
                    + "AIMAR_Conductores.Des_con, "
                    + "AIMAR_Conductores.Bre_con, "
                    + "AIMAR_Conductores.Not_con "
                    + "FROM AIMAR_Conductores;";

            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                conductor = new AimarConductores();
                conductor.setDniCon(rs.getString(1));
                conductor.setAbrCon(rs.getString(2));
                conductor.setDesCon(rs.getString(3));
                conductor.setBreCon(rs.getString(4));
                conductor.setNotCon(rs.getString(5));
                
                lista.add(conductor);
            }
            if (!existe) {
                conductor = null;
                System.out.println("No se encontro datos");
            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
        return lista;

    }

}
