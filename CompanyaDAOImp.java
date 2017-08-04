package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import sys.dao.CompanyaDAO;
import sys.model.AgmaeCentrocosto;
import sys.model.AgmaeCompanya;
import sys.model.AimarAlmacen;
import sys.util.Service;

public class CompanyaDAOImp implements CompanyaDAO {

    @Override
    public List<AgmaeCompanya> listarCompanyas() {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AgmaeCompanya comp = null;
        List<AgmaeCompanya> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT ruc_companyia, des_companyia "
                    + "FROM agmae_companyias;";

            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                comp = new AgmaeCompanya();
                comp.setRucCompanya(rs.getString("ruc_companyia"));
                comp.setDesCompanyia(rs.getString("des_companyia"));

                lista.add(comp);
            }
            if (!existe) {
                comp = null;
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

    @Override
    public AgmaeCompanya consultarObjCompanya(String rucCompanya) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        AgmaeCompanya comp = null;

        try {
            cn = Service.getConexion();
            String sql = "SELECT ruc_companyia, des_companyia "
                    + "FROM agmae_companyias "
                    + "WHERE ruc_companyia=?;";

            ps = cn.prepareStatement(sql);
            ps.setString(1, rucCompanya);
            rs = ps.executeQuery();

            comp = new AgmaeCompanya();
            if (rs.next()) {
                comp.setRucCompanya(rs.getString("ruc_companyia"));
                comp.setDesCompanyia(rs.getString("des_companyia"));
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
        return comp;
    }

}
